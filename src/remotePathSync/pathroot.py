from __future__ import annotations
import paramiko
from scp import SCPClient
from os.path import join as opj, exists as ope
from os import listdir
from datetime import datetime
import getpass
from cryptography.fernet import Fernet
import os
import errno
import stat
from pathlib import Path
import shutil

key = Fernet.generate_key()
fernet = Fernet(key)


def get_common_path_multi(directory_paths: list[Path]):
    sample_path = directory_paths[0]
    common_path = sample_path
    for path in directory_paths:
        common_path = get_common_path(path, common_path)
    return common_path


def get_common_path(path1: Path, path2: Path):
    other = path1.with_segments(path2)
    for step, path in enumerate([other] + list(other.parents)):
        if path1.is_relative_to(path):
            return path

def createSSHClient(server, port, user, password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, user, password)
    return client

def get_pw_and_otp_combo():
    encPWOTP = fernet.encrypt(getpass.getpass('Password + OTP:').encode())
    return encPWOTP

def createSSHClient_through_agent(hostname: str, username: str) -> paramiko.SSHClient | None:
    
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    # Check for SSH agent
    if "SSH_AUTH_SOCK" not in os.environ:
        return None
        
    try:
        # Get keys from agent
        ssh_agent = paramiko.Agent()
        agent_keys = ssh_agent.get_keys()
        
        # Try each key
        for key in agent_keys[::-1]: # ssh_agent.get_keys() returns a ton of keys, most of which are old and unusable - reversing order prioritizes most recent keys
            try:
                client.connect(
                    hostname=hostname,
                    username=username,
                    pkey=key
                )
                return client
            except Exception as e:
                continue
                
        # If we get here, no keys worked
        return None
        
    except Exception:
        # Handle any other errors (like agent connection failing)
        return None


def get_scp_and_ssh(username: str, hostname: str, try_agent=True):
    """
    Get SCPClient and SSHClient for given username and hostname

    Args:
        username (str): Username for SSH connection
        hostname (str): Hostname for SSH connection (ie 'login.rc.colorado.edu')
        try_agent (bool): Whether to try using SSH agent for authentication
    """
    port = 22
    ssh = None
    if try_agent:
        ssh = createSSHClient_through_agent(hostname, username)
    if ssh is None:
        sdfsdfsdf = get_pw_and_otp_combo()
        ssh = createSSHClient(hostname, port, username, fernet.decrypt(sdfsdfsdf).decode())
        del sdfsdfsdf
    scp = SCPClient(ssh.get_transport())
    return scp, ssh

class PathRoot:

    remote = False
    root: Path | None = None
    ssh: paramiko.SSHClient | None = None
    scp: SCPClient | None = None
    sftp: paramiko.SFTPClient | None = None
    hostname: str | None = None

    def __init__(self, root: Path, hostname: str | None, try_agent=True, _ssh=None, username: str | None = None, keepalive_interval: int | None = 60, port: int = 22):
        self.root = root
        ssh = _ssh
        scp = None
        self.sftp = None
        if not hostname is None:
            self.remote = True
            if username is None:
                raise ValueError("username must be provided for remote PathRoot")
            if _ssh is None:
                if try_agent:
                    ssh = createSSHClient_through_agent(hostname, username)
                if ssh is None:
                    sdfsdfsdf = get_pw_and_otp_combo()
                    ssh = createSSHClient(hostname, port, username, fernet.decrypt(sdfsdfsdf).decode())
                    del sdfsdfsdf
            scp = SCPClient(ssh.get_transport())
            self.hostname = hostname
            self.scp = scp
            self.ssh = ssh
            self.set_keepalive(keepalive_interval)
        else:
            self.remote = False

    def set_keepalive(self, interval: int | None):
        """ Set keepalive interval for remote SSH connection """
        if not interval is None:
            transport = self.ssh.get_transport()
            transport.set_keepalive(interval)

    def from_pathroot(cls, root: Path, pathroot: PathRoot):
        hostname = None
        if pathroot.remote:
            hostname = pathroot.hostname
        instance = cls(root, hostname, _ssh = pathroot.ssh)
        return instance

    def _get_sftp(self) -> paramiko.SFTPClient:
        if not self.remote:
            raise ValueError("SFTP is only available for remote PathRoot instances")
        if self.sftp is None:
            self.sftp = self.ssh.open_sftp()
        return self.sftp

    def run(self, cmd):
        if self.remote:
            out = self.ssh.exec_command(cmd)
            return out[1].read().decode()
        else:
            return os.popen(cmd).read()
        
    def make_zip(self, arb_dir_path: Path, exclude_fs: list[str] | None = None, include_fs=None) -> Path:
        zip_path = arb_dir_path.parent / f"{str(arb_dir_path.name)}.zip"
        cmd = f"cd {str(arb_dir_path.parent)}; zip -r {str(arb_dir_path.name)}.zip {str(arb_dir_path.name)}"
        app_files = None
        if include_fs is not None:
            cmd += " -i"
            app_files = [include_fs] if isinstance(include_fs, str) else include_fs
        elif exclude_fs is not None:
            cmd += " -x"
            app_files = [exclude_fs] if isinstance(exclude_fs, str) else exclude_fs
        if not app_files is None:
            for f in app_files:
                cmd += f" '*/{f}'"
        self.run(cmd)
        return zip_path
    
    def make_zip_multi(self, directory_paths: list[Path], exclude_fs: list[str] | None = None, zip_name = "archive.zip", pref_common_path: Path | None = None) -> Path:
        common_path = get_common_path_multi(directory_paths)
        if not pref_common_path is None:
            if not common_path.is_relative_to(pref_common_path):
                raise ValueError(f"Provided pref_common_path ({pref_common_path}) is not relative to minimal common path of provided directories ({common_path})")
            else:
                common_path = pref_common_path
        rel_dirs = [d.relative_to(common_path) for d in directory_paths]
        cmd = f"cd {common_path}; zip -r {zip_name} . -i"
        for reld in rel_dirs:
            cmd += f" '{reld / "*"}'"
        if not exclude_fs is None:
            cmd += " -x"
            for f in exclude_fs:
                cmd += f" '*/{f}'"
        self.run(cmd)
        return common_path / zip_name
    
    def unzip(self, zip_path: Path, overwrite_existing=True):
        cmd = f"cd {str(zip_path.parent)}; "
        cmd += "unzip -o " if overwrite_existing else "unzip "
        cmd += f"{str(zip_path.name)}"
        self.run(cmd)


    # Replace ls parsing with using jc library
    def get_ls_fs(self, path: Path):
        """ Return list of all files and directories in path """
        _fs = self.run(f"ls -a '{path}'")
        fs = _fs.split('\n')
        fs = [f for f in fs if not f in [".", "..", '']]
        return fs
    
    def get_ls_l_fs(self, path: Path):
        """ Return a dictionary of files and directories in path with ls -l data """
        # path = path.replace("\\", "/")
        fs = self.get_ls_fs(path)
        data = {}
        _ls_l = self.run(f"ls -a -l --time-style=long-iso '{path}'")
        nsplit_ls_l = _ls_l.split("\n")
        for line in nsplit_ls_l:
            contained_fs = [f for f in fs if line.endswith(f)]
            f = None
            if len(contained_fs) > 1:
                # Choose longest match
                f = max(contained_fs, key=len)
            elif len(contained_fs) == 1:
                f = contained_fs[0]
            if not f is None:
                # Incase there are split characters in the file name
                if line.count(f) == 1:
                    splitline = line.split(f)[0].split()
                else:
                    splitline = line.rstrip(f).split()
                data[f] = splitline
        return data
    
    def get_ls_l_file_info(self, path: str):
        """ Return a dictionary of files and directories in path extracted from ls -l data """
        path = path.replace("\\", "/")
        ls_l_data = self.get_ls_l_fs(path)
        file_info = {}
        for f in ls_l_data:
            if len(ls_l_data[f]):
                file_info[f] = {}
                file_info[f]["isdir"] = ls_l_data[f][0][0] == "d"
                file_info[f]["size"] = ls_l_data[f][4]
                try:
                    timeo = datetime.fromisoformat(f"{ls_l_data[f][5]}T{ls_l_data[f][6]}:00")
                except IndexError as e:
                    print(f"Error parsing time for {f}: {ls_l_data[f]}")
                    raise e
                file_info[f]["time"] = timeo
        return file_info

    def ope(self, path: Path):
        """ Fast remote existence check using SFTP stat instead of shell output. """
        if self.remote:
            try:
                self._get_sftp().stat(str(path))
                return True
            except OSError as exc:
                if exc.errno == errno.ENOENT:
                    return False
                raise
        else:
            return ope(path)

    def isdir(self, path: Path):
        """ Fast remote directory check using SFTP stat instead of shell output. """
        if self.remote:
            try:
                mode = self._get_sftp().stat(str(path)).st_mode
                return stat.S_ISDIR(mode)
            except OSError as exc:
                if exc.errno == errno.ENOENT:
                    return False
                raise
        else:
            return path.is_dir()
        
    def rm(self, path: Path):
        if not path.is_relative_to(self.root):
            raise ValueError(f"Path {path} does not contain root {self.root}")
        if not self.remote:
            if path.is_dir():
                return shutil.rmtree(path)
            return path.unlink()
        else:
            if self.isdir(path):
                self._get_sftp().rmdir(str(path))
            else:
                self._get_sftp().remove(str(path))
            return None
        
    def mkdir(self, path: Path, parents=True, exist_ok=True):
        if not path.is_relative_to(self.root):
            raise ValueError(f"Path {path} does not contain root {self.root}")
        if not self.remote:
            return path.mkdir(parents=True, exist_ok=True)
        else:
            self._get_sftp().mkdir(str(path), parents=parents, exist_ok=exist_ok)
    
    def listdirs(self, path):
        if self.remote:
            fs = self.get_ls_l_file_info(path)
            fs = [f for f in fs if fs[f]["isdir"]]
            return fs
        else:
            return listdir(path)