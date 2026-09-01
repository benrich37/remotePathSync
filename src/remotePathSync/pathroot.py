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
from subprocess import Popen, PIPE

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

def createSSHClient(server, port, user, password, look_for_keys=True, allow_agent=True):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    if look_for_keys or allow_agent:
        
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, user, password, look_for_keys=look_for_keys, allow_agent=allow_agent)
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
    proc : Popen | None = None
    proc_alias: str | None = None
    root: Path | None = None
    ssh: paramiko.SSHClient | None = None
    scp: SCPClient | None = None
    sftp: paramiko.SFTPClient | None = None
    hostname: str | None = None
    transfer_pathroot: PathRoot | None = None
    

    def __init__(
            self, root: Path, hostname: str | None, proc_alias: str | None = None, try_agent=True, 
            _ssh=None, username: str | None = None, keepalive_interval: int | None = 60, 
            port: int = 22, proc_okay: bool = True, transfer_pathroot: PathRoot | None = None
            ):
        self.root = root
        self.proc_okay = proc_okay
        # self.proc_alias = proc_alias
        if not proc_alias is None:
            self.proc_alias = proc_alias
        else:
            self.proc_alias = hostname
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
                    print(f"Connecting to {username}@{hostname}:{port} with password + OTP authentication...")
                    ssh = createSSHClient(
                        hostname, port, username, fernet.decrypt(sdfsdfsdf).decode(), 
                        allow_agent=try_agent,
                        look_for_keys=try_agent
                        )
                    del sdfsdfsdf
            scp = SCPClient(ssh.get_transport())
            self.hostname = hostname
            self.scp = scp
            self.ssh = ssh
            self.set_keepalive(keepalive_interval)
            if not transfer_pathroot is None:
                self.transfer_pathroot = transfer_pathroot
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
    

    # note to self for future - this will probably fail for alpine as it might automatically try to use my bajillion ssh keys and fail to connect instead of prompting for password - may need to add option to disable trying agent or limit number of keys tried from agent
    def open_proc(self):
        if self.remote:
            if self.proc is None:
                self.proc = Popen(["ssh", self.proc_alias], stdin=PIPE, stdout=PIPE, stderr=PIPE, text=True)
                self._proc_run("pwd")
                # self._proc_run_verbose("pwd")
        else:
            raise ValueError("Process opening is only available for remote PathRoot instances")
        
    def close_proc(self):
        if self.remote:
            if not self.proc is None:
                self.proc.stdin.write("exit\n")
                self.proc.stdin.flush()
                self.proc.wait()
                self.proc = None
        else:
            raise ValueError("Process closing is only available for remote PathRoot instances")

    def _get_sftp(self) -> paramiko.SFTPClient:
        if not self.remote:
            raise ValueError("SFTP is only available for remote PathRoot instances")
        use = self
        if not self.transfer_pathroot is None:
            use = self.transfer_pathroot
        if use.sftp is None:
            use.sftp = use.ssh.open_sftp()
        return use.sftp

    def run(self, cmd, decode=True):
        if self.remote:
            out = self.ssh.exec_command(cmd)
            if decode:
                return out[1].read().decode()
            else:
                return "(decoding suppressed)"
        else:
            return os.popen(cmd).read()
        
    def _proc_run(self, cmd, sentinel="__DONE__"):
        self.proc.stdin.write(f"{cmd}; echo {sentinel}\n")
        self.proc.stdin.flush()
        output = []
        for line in self.proc.stdout:
            if line.strip() == sentinel:
                break
            output.append(line)
        return "".join(output)
    
    def _proc_run_verbose(self, cmd, sentinel="__DONE__"):
        self.proc.stdin.write(f"{cmd}; echo {sentinel}\n")
        self.proc.stdin.flush()
        stdout = []
        stderr = []
        for line in self.proc.stdout:
            if line.strip() == sentinel:
                break
            stdout.append(line)
        for line in self.proc.stderr:
            stderr.append(line)
        print("STDOUT:")
        print("".join(stdout))
        print("STDERR:")
        print("".join(stderr))
        return "".join(stdout)
        
    def proc_run(self, cmd, verbose=False):
        if self.remote:
            if not self.proc_okay:
                return self.run(cmd)
            self.open_proc()
            if verbose:
                return self._proc_run_verbose(cmd)
            else:
                return self._proc_run(cmd)
        else:
            raise ValueError("Process running is only available for remote PathRoot instances")
        
    def run_return_object(self, cmd: str) -> os._wrap_close | tuple[paramiko.ChannelFile, paramiko.ChannelFile, paramiko.ChannelFile]:
        if self.remote:
            return self.ssh.exec_command(cmd)
        else:
            return os.popen(cmd)
        
    def make_zip(self, arb_dir_path: Path, exclude_fs: list[str] | None = None, include_fs=None) -> Path:
        zip_path = arb_dir_path.parent / f"{str(arb_dir_path.name)}.zip"
        if zip_path.exists():
            zip_path.unlink()
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
        print(cmd)
        self.run(cmd)
        return zip_path
    
    def make_zip_multi(self, directory_paths: list[Path], exclude_fs: list[str] | None = None, include_fs: list[str] | None = None, zip_name = "archive.zip", pref_common_path: Path | None = None) -> Path:
        common_path = get_common_path_multi(directory_paths)
        if not pref_common_path is None:
            if not common_path.is_relative_to(pref_common_path):
                raise ValueError(f"Provided pref_common_path ({pref_common_path}) is not relative to minimal common path of provided directories ({common_path})")
            else:
                common_path = pref_common_path
        rel_dirs = [d.relative_to(common_path) for d in directory_paths]
        cmd = f"cd {common_path}; zip -r {zip_name}"
        for reld in rel_dirs:
            cmd += f" {reld}"
        if not include_fs is None:
            cmd += " -i"
            for f in include_fs:
                cmd += f" '*/{f}'"
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
        # _fs = self.run(f"ls -a '{path}'")
        _fs = self.proc_run(f"ls -a '{path}'")
        fs = _fs.split('\n')
        fs = [f for f in fs if not f in [".", "..", '']]
        return fs
    
    def get_ls_l_fs(self, path: Path):
        """ Return a dictionary of files and directories in path with ls -l data """
        # path = path.replace("\\", "/")
        fs = self.get_ls_fs(path)
        data = {}
        # _ls_l = self.run(f"ls -a -l --time-style=long-iso '{path}'")
        _ls_l = self.proc_run(f"ls -a -l --time-style=long-iso '{path}'")
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
        path = str(path)
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

    def _remote_path_exists_or_raise(self, path: Path, exist_ok: bool):
        if self.ope(path):
            if not self.isdir(path):
                raise NotADirectoryError(errno.ENOTDIR, os.strerror(errno.ENOTDIR), str(path))
            if not exist_ok:
                raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), str(path))
            return True
        return False

    def _mkdir_remote_single(self, path: Path, exist_ok: bool):
        if self._remote_path_exists_or_raise(path, exist_ok):
            return None
        self._get_sftp().mkdir(str(path))
        return None

    def _mkdir_remote_parents(self, path: Path, exist_ok: bool):
        sftp = self._get_sftp()
        if self._remote_path_exists_or_raise(path, exist_ok):
            return None

        current = self.root
        relative_parts = path.relative_to(self.root).parts
        for index, part in enumerate(relative_parts):
            current = current / part
            is_final = index == len(relative_parts) - 1
            if self.ope(current):
                if not self.isdir(current):
                    raise NotADirectoryError(errno.ENOTDIR, os.strerror(errno.ENOTDIR), str(current))
                if is_final and not exist_ok:
                    raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), str(current))
                continue
            sftp.mkdir(str(current))
        return None
        
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
        
    def mv(self, src: Path, dst: Path):
        if not src.is_relative_to(self.root):
            raise ValueError(f"Source path {src} does not contain root {self.root}")
        if not dst.is_relative_to(self.root):
            raise ValueError(f"Destination path {dst} does not contain root {self.root}")
        if not self.remote:
            return shutil.move(src, dst)
        else:
            self._get_sftp().rename(str(src), str(dst))
            return None
        
    def mkdir(self, path: Path, parents=True, exist_ok=True):
        if not path.is_relative_to(self.root):
            raise ValueError(f"Path {path} does not contain root {self.root}")
        if not self.remote:
            return path.mkdir(parents=parents, exist_ok=exist_ok)
        if not parents:
            return self._mkdir_remote_single(path, exist_ok)
        return self._mkdir_remote_parents(path, exist_ok)
    
    def listdirs(self, path):
        if self.remote:
            fs = self.get_ls_l_file_info(path)
            fs = [f for f in fs if fs[f]["isdir"]]
            return fs
        else:
            return listdir(path)

    # def listdirs(self, path: Path):