from __future__ import annotations
from os.path import getatime, join as opj, exists as ope, isdir
from os import listdir
from datetime import datetime
import time
import os
from pathlib import Path
from shutil import copy2 as cp
from remotePathSync.pathroot import PathRoot


class PathRootPair:
    local: PathRoot
    local_root: Path | str | None = None
    local_roots: dict[str, str | Path] | None = None
    remote: PathRoot
    remote_root: Path | str | None = None
    remote_roots: dict[str, str | Path] | None = None
    job_cache: dict[str, dict] = {}
    job_cache_refresh_time: float = 60
    hostname: str | None = None
    username: str | None = None
    hostnames: dict[str, str] | None = None
    usernames: dict[str, str] | None = None
    # Figure out how to make this customizable
    submit_command_template: str = "cd {path}; sbatch {slurm_file_name}"
    slurm_file_name: str = "psubmit.sh"
    exclude_fs_default = ["wfns", "n_up", "n_dn", "fluidState", "out_wforce.logx", "force"]


    def __init__(self, local: PathRoot, remote: PathRoot):
        self.local = local
        self.remote = remote

    @classmethod
    def from_paths(
        cls, 
        cluster: str | None = None,
        local_root: Path | None = None, 
        local_roots: dict[str, str | Path] | None = None,
        remote_root: Path | None = None, 
        remote_roots: dict[str, str | Path] | None = None,
        hostname: str | None = None, 
        hostnames: dict[str, str] | None = None,
        username: str | None = None,
        usernames: dict[str, str] | None = None,
        keepalive_interval: int | None = 60, 
        try_agent=True, 
        ):
        # TODO: Refactor to reduce redundancy with all this checking
        if (local_roots is None) and (not cls.local_roots is None):
            local_roots = cls.local_roots
        if local_root is None:
            if not cluster is None:
                assert cluster in local_roots, f"Cluster {cluster} not found in local_roots dictionary ({local_roots})"
                local_root = Path(local_roots[cluster])
            else:
                raise ValueError("local_root must be provided either directly or through local_roots dictionary and cluster name")
        if (remote_roots is None) and (not cls.remote_roots is None):
            remote_roots = cls.remote_roots
        if remote_root is None:
            if not cluster is None:
                assert cluster in remote_roots, f"Cluster {cluster} not found in remote_roots dictionary ({remote_roots})"
                remote_root = Path(remote_roots[cluster])
            else:
                raise ValueError("remote_root must be provided either directly or through remote_roots dictionary and cluster name")
        if username is None and (not cls.username is None):
            username = cls.username
        if (usernames is None) and (not cls.usernames is None):
            usernames = cls.usernames
        if (username is None) and (not usernames is None):
            if not cluster is None:
                assert cluster in usernames, f"Cluster {cluster} not found in usernames dictionary ({usernames})"
                username = usernames[cluster]
            else:
                raise ValueError("username must be provided either directly or through usernames dictionary and cluster name")
        if hostname is None and (not cls.hostname is None):
            hostname = cls.hostname
        if (hostnames is None) and (not cls.hostnames is None):
            hostnames = cls.hostnames
        if (hostname is None) and (not hostnames is None):
            if not cluster is None:
                assert cluster in hostnames, f"Cluster {cluster} not found in hostnames dictionary ({usernames})"
                hostname = hostnames[cluster]
            else:
                raise ValueError("hostname must be provided either directly or through hostnames dictionary and cluster name")
        print(f"Connecting to {hostname} (user: {username}, remote root: {remote_root}, local root: {local_root})")
        local = PathRoot(local_root, None)
        remote = PathRoot(remote_root, hostname, try_agent, username=username)
        instance = cls(local, remote)
        if not keepalive_interval is None:
            instance.set_keepalive(keepalive_interval)
        return instance
    
    def reconnect(self, try_agent=True):
        hostname = self.remote.hostname
        self.remote = PathRoot(self.remote.root, hostname, try_agent)
        

    def set_keepalive(self, interval: int = 60):
        """ Set keepalive interval for remote SSH connection """
        self.remote.set_keepalive(interval)
    
    def get_local_path(self, remote_path: Path):
        """ Get local path from remote path (<remote_root>/<path tree> -> <local_root>/<path tree>) """
        return self.local.root / remote_path.relative_to(self.remote.root)
    
    def get_remote_path(self, local_path: str):
        """ Get remote path from local path (<local_root>/<path tree> -> <remote_root>/<path tree>) """
        return self.remote.root / local_path.relative_to(self.local.root)
        
    def get_local_remote_from_arb(self, arb_path: Path):
        if arb_path.is_relative_to(self.local.root):
            local_path = arb_path
            remote_path = self.get_remote_path(arb_path)
            return local_path, remote_path
        elif arb_path.is_relative_to(self.remote.root):
            remote_path = arb_path
            local_path = self.get_local_path(arb_path)
            return local_path, remote_path
        else:
            raise ValueError(f"Path {arb_path} is not relative to either local root {self.local.root} or remote root {self.remote.root}")

    def download(self, arb_path: Path | str, p=True):
        local_file, remote_file = self.get_local_remote_from_arb(Path(arb_path))
        self.local.mkdir(local_file.parent)
        if p:
            print(f"{remote_file} --> {local_file}")
        self.remote.scp.get(str(remote_file), str(local_file))
    

    def zip_download(self, arb_path: Path, p: bool = True, exclude_fs=["wfns"], include_fs=None):
        self.zip_transfer(arb_path, True, p=p, exclude_fs=exclude_fs, include_fs=include_fs)

    def zip_upload(self, arb_path: Path, p: bool = True, exclude_fs=["wfns"], include_fs=None):
        self.zip_transfer(arb_path, False, p=p, exclude_fs=exclude_fs, include_fs=include_fs)

    def zip_transfer(self, arb_path: Path, download: bool, p: bool = True, exclude_fs=["wfns"], include_fs=None, overwrite_existing=True):
        ret_step = -1 if download else 1
        uploader, downloader = (self.local, self.remote)[::ret_step]
        upload_dir, download_dir = self.get_local_remote_from_arb(arb_path)[::ret_step]
        upload_zip, download_zip = self.get_local_remote_from_arb(
                uploader.make_zip(upload_dir, exclude_fs=exclude_fs, include_fs=include_fs)
                )[::ret_step]
        _ = self.download(download_zip, p=p) if download else self.upload(upload_zip, p=p)
        uploader.rm(upload_zip)
        downloader.unzip(download_zip, overwrite_existing=overwrite_existing)
        downloader.rm(download_zip)


    def download_dir(self, arb_path: Path, p: bool = True, as_zip: bool = True,
                     exclude_fs: list[str] | None = None,
                     include_fs=None, update_existing=True):
        if exclude_fs is None:
            exclude_fs = self.exclude_fs_default
        if not as_zip:
            self.update_dir_contents(
                Path(arb_path),
                p=p, force_download=update_existing,
                )
        else:
            self.zip_download(Path(arb_path), p=p, exclude_fs=exclude_fs, include_fs=include_fs)
            

    def upload_dir(self, arb_path: Path, p: bool = True, as_zip: bool = True,
                     exclude_fs: list[str] | None = None,
                     include_fs=None, update_existing=True):
        if exclude_fs is None:
            exclude_fs = self.exclude_fs_default
        if not as_zip:
            self.upload_recursive(
                Path(arb_path),
                p=p,
                )
        else:
            self.zip_upload(Path(arb_path), p=p, exclude_fs=exclude_fs, include_fs=include_fs)
            

    def upload(self, arb_file_path: Path | str, p=True):
        local_file, remote_file = self.get_local_remote_from_arb(Path(arb_file_path))
        msg = self.remote.mkdir(remote_file.parent)
        if p:
            print(msg)
            print(f"{local_file} --> {remote_file}")
        self.remote.scp.put(str(local_file), str(remote_file.parent))

    def uploads(self, arb_dir_path: Path, file_list: list[str], p=True):
        local_dir, remote_dir = self.get_local_remote_from_arb(arb_dir_path)
        if p:
            print(f"{file_list}: {local_dir} --> {remote_dir}")
        self.remote.scp.put(" ".join([str(local_dir / f) for f in file_list]), remote_dir)

    def upload_recursive(self, arb_path: Path, p=True):
        local_path, remote_path = self.get_local_remote_from_arb(arb_path)
        self.remote.mkdir(remote_path)
        subfiles = [f for f in listdir(local_path) if not (local_path / f).is_dir()]
        subfiles = [f for f in subfiles if not f.startswith("._")]
        subdirs = [f for f in listdir(local_path) if (local_path / f).is_dir()]
        for f in subfiles:
            self.upload(local_path / f, p=p)
        for d in subdirs:
            self.upload_recursive(local_path / d, p=p)

    def write_dir_updated_timestamp(self, path):
        with open(opj(path, "last_updated.txt"), "w") as f:
            f.write(str(float(time.time())))
        f.close()

    def update_dir_contents(
            self, arb_dir: Path, 
            exclude_fs: list[str] | None = None,
            p=True, 
            include_fs=None,
            recursive=True,
            force_download=False,
            ):
        if exclude_fs is None:
            exclude_fs = self.exclude_fs_default
        local_dir, remote_dir = self.get_local_remote_from_arb(arb_dir)
        local_dir.mkdir(parents=True, exist_ok=True)
        remote_files = self.remote.get_ls_l_file_info(remote_dir)
        local_files = self.local.get_ls_fs(local_dir)
        download_fs = list(remote_files.keys())
        download_fs = [f for f in download_fs if not remote_files[f]["isdir"]]
        download_fs = [f for f in download_fs if not f in exclude_fs]
        if not include_fs is None:
            download_fs = [f for f in download_fs if f in include_fs]
        need_fs = [f for f in download_fs if (not f in local_files)]
        update_fs = [f for f in download_fs if not f in need_fs]
        if not force_download:
            _update_fs = []
            for f in update_fs:
                local_time = datetime.fromtimestamp(getatime(opj(local_dir, f)))
                remote_time = remote_files[f]["time"]
                local_size = os.path.getsize(opj(local_dir, f))
                remote_size = remote_files[f]["size"]
                outdated = local_time < remote_time
                dif_size = local_size != int(remote_size)
                if outdated or dif_size:
                    _update_fs.append(f)
            update_fs = _update_fs
        if len(need_fs) or len(update_fs):
            print(f"Updating {remote_dir} --> {local_dir}")
            print(f"Updating files {update_fs}")
            print(f"Downloading files {need_fs}")
            download_fs = update_fs + need_fs
            for f in download_fs:
                self.download(remote_dir / f, p=p)
        self.write_dir_updated_timestamp(local_dir)
        if recursive:
            remote_dirs = [f for f in remote_files if remote_files[f]["isdir"]]
            for d in remote_dirs:
                self.update_dir_contents(
                    local_dir / d,
                    exclude_fs=exclude_fs,
                    include_fs=include_fs,
                    recursive=recursive,
                    force_download=force_download,
                    p=p
                    )

    def get_dir_updated_timestamp(self, local_path: Path):
        fname = local_path / "last_updated.txt"
        last_updated = None
        if fname.exists():
            timestamp = ""
            with open(fname, "r") as f:
                for line in f:
                    timestamp = line
            last_updated = float(timestamp.strip())
        return last_updated

        
    def need_check(self, local_path: Path, day_freq, force_check=False):
        if force_check:
            return True
        else:
            cur = time.time()
            last = self.get_dir_updated_timestamp(local_path)
            if last is None:
                return True
            else:
                elap = cur - last
                df2s = day_freq*24*60*60
                return elap > df2s
            
    def get_user(self):
        home = self.remote.run("cd $HOME; pwd")
        return home.strip().split("/")[-1]
    
    def parse_slurm_line(self, line):
        l = line.split()
        path = l[1]
        slurm_line = {
            "job type": l[0],
            "jobid": l[2],
            "state": l[3],
            "elapsed": l[4]
        }
        return path, slurm_line
    
    def get_slurm_jobs(self, days=1, exclude_cancelled=True):
        slurm_job_history = self.get_slurm_job_history(days=days)
        slurm_jobs = {}
        for path in slurm_job_history:
            latest_job = slurm_job_history[path][-1]
            if not exclude_cancelled or (not "CANCELLED" in latest_job["state"]):
                slurm_jobs[path] = latest_job
        return slurm_jobs


    def get_slurm_job_history(self, days=1):
        return self.access_job_cache(days=days)
    
    def _get_slurm_jobs_history(self, days=1):
        user = self.get_user()
        cmd = f'sacct -X -u {user} -S $(date -d "{days} days ago" +%D-%R) --format=jobname%-100,workdir%-200,jobid,state,elapsed'
        out = self.remote.run(cmd).strip().split("\n")[2:]
        slurm_jobs = {}
        for line in out:
            path, info = self.parse_slurm_line(line)
            if not path in slurm_jobs:
                slurm_jobs[path] = []
            slurm_jobs[path].append(info)
        return slurm_jobs
    
    def update_job_cache(self, days=1):
        self.job_cache[str(days)] = {
            "slurm_jobs": self._get_slurm_jobs_history(days=days),
            "time": time.time()
        }

    def access_job_cache(self, days=1):
        cur_time = time.time()
        if (str(days) not in self.job_cache) or ((cur_time - self.job_cache[str(days)]["time"]) > self.job_cache_refresh_time):
            self.update_job_cache(days=days)
        return self.job_cache[str(days)]["slurm_jobs"]
    
    def get_job_state(self, path: Path, check_days=3) -> str | None:
        """ Get the slurm STATE of a job associated with a local or remote path """
        path = str(self.get_local_remote_from_arb(path)[1])
        slurm_jobs = self.get_slurm_jobs(days=check_days)
        curstate = None
        if path in slurm_jobs:
            curstate = slurm_jobs[path]["state"]
        return curstate
    
    def get_job_states(self, path: Path, check_days=3) -> str | None:
        """ Get the slurm STATE of a job associated with a local or remote path """
        path = str(self.get_local_remote_from_arb(path)[1])
        slurm_jobs_history = self.get_slurm_jobs_history(days=check_days)
        curstates = []
        if path in slurm_jobs_history:
            curstates = [slurm_jobs_history[path][i]["state"] for i in range(len(slurm_jobs_history[path]))]
        return curstates
    
    def path_is_currently_running(self, path: Path, check_days=3):
        curstate = self.get_job_state(path, check_days=check_days)
        if curstate is None:
            return False
        else:
            return "RUNNING" in curstate

    def path_is_currently_pending(self, path: Path, check_days=3):
        curstate = self.get_job_state(path, check_days=check_days)
        if curstate is None:
            return False
        else:
            return "PENDING" in curstate

    def path_is_on_slurm_queue(self, path: Path, check_days=3):
        return self.get_job_state(path, check_days=check_days) in ["PENDING", "RUNNING"]
    
    def submit_local_path(self, arb_path: Path, check_days=3, force_submit=False, update_local=True, as_zip: bool = True):
        local_path, remote_path = self.get_local_remote_from_arb(arb_path)
        job_state = self.get_job_state(remote_path, check_days=check_days)
        if isinstance(job_state, str):
            if job_state == "RUNNING":
                print(f"Job is currently running: {remote_path}")
                return
            if job_state == "PENDING":
                print(f"Job is currently pending: {remote_path}")
                if force_submit:
                    print("Cancelling preexisting job")
                    job_id = self.get_slurm_jobs(days=check_days)[remote_path]["jobid"]
                    self.cancel_jobid(job_id)
                else:
                    print("Use force_submit=True to cancel preexisting job")
                    return
            if job_state == "COMPLETED":
                print(f"Job is already completed: {remote_path}")
                if force_submit:
                    print("Resubmitting job")
                else:
                    print("Use force_submit=True to resubmit job")
                    return
            if job_state == "TIMEOUT":
                print(f"Job has timed out: {remote_path}")
                if update_local:
                    self.download_dir(arb_path, as_zip=as_zip)
        self.upload_dir(local_path, as_zip=as_zip, update_existing=True)
        self.submit_path_psubmit(remote_path)


    def submit_path_psubmit(self, path: Path, slurm_file_name: str | None = None):
        path = str(self.get_local_remote_from_arb(path)[1])
        if slurm_file_name is None:
            slurm_file_name = self.slurm_file_name
        print(self.remote.run(self.submit_command_template.format(path=path, slurm_file_name=slurm_file_name)))

    def cancel_jobid(self, jobid):
        print(self.remote.run(f"scancel {jobid}"))




class LocalPathRootPair:
    local1: PathRoot
    local2: PathRoot

    def __init__(self, local1: PathRoot, local2: PathRoot):
        self.local1 = local1
        self.local2 = local2

    @classmethod
    def from_paths(cls, local1: str, local2: str):
        local1 = PathRoot(local1, None)
        local2 = PathRoot(local2, None)
        instance = cls(local1, local2)
        return instance
    
    def get_local1_path(self, local2_path: str):
        """ Get local path from local2 path (<local2_root>/<path tree> -> <local_root>/<path tree>) """
        local2_root = self.local2.root
        local_root = self.local1.root
        local2_path = local2_path.replace(local2_root, "")
        local_path = opj(local_root, local2_path)
        return local_path
    
    def get_local2_path(self, local1_path: str):
        """ Get local2 path from local path (<local_root>/<path tree> -> <local2_root>/<path tree>) """
        local1_root = self.local1.root
        local2_root = self.local2.root
        local1_path = local1_path.replace(local1_root, "")
        local2_path = opj(local2_root, local1_path)
        return local2_path
        
    
    def download(self, local2_file_path: str, local1_dir_path: str | None = None, p=True):
        if local1_dir_path is None:
            local1_dir_path = self.get_local1_path(local2_file_path)
        if p:
            print(f"{local2_file_path} --> {local1_dir_path}")
        cp(local2_file_path, local1_dir_path)


    def upload(self, local1_file_path, local2_dir_path: str | None = None, p=True):
        if local2_dir_path is None:
            local2_dir_path = self.get_local2_path(local1_file_path)
        if p:
            print(f"{local1_file_path} --> {local2_dir_path}")
        cp(local1_file_path, local2_dir_path)

    def upload_recursive(self, local1_file_path, p=True):
        local2_dir_path = self.get_local2_path(local1_file_path)
        if not self.local2.ope(local2_dir_path):
            self.local2.mkdir(local2_dir_path)
        for f in listdir(local1_file_path):
            if isdir(opj(local1_file_path, f)):
                self.upload_recursive(opj(local1_file_path, f), p=p)
            else:
                self.upload(opj(local1_file_path, f), local2_dir_path, p=p)

    def write_dir_updated_timestamp(self, path):
        with open(opj(path, "last_updated.txt"), "w") as f:
            f.write(str(float(time.time())))
        f.close()


    def sync_dir_contents(
            self, local1_dir, local2_dir = None,
            exclude_fs: list[str] | None = None,
            p=True, 
            exclude_dirs=None,
            include_fs=None,
            recursive=True,
            force_download=False,
            ):
        if exclude_fs is None:
            exclude_fs = []
        if local2_dir is None:
            local2_dir = self.get_local2_path(local1_dir)
        self.update_dir_contents(local1_dir, local2_dir=local2_dir, exclude_fs=exclude_fs, include_fs=include_fs, recursive=recursive, force_download=force_download, p=p, exclude_dirs=exclude_dirs)
        self.update_dir_contents(local2_dir, local2_dir=local1_dir, exclude_fs=exclude_fs, include_fs=include_fs, recursive=recursive, force_download=force_download, p=p, reverse=True, exclude_dirs=exclude_dirs)
        
                

    def update_dir_contents_helper(
            self, local1_dir: str, local2_dir: str, local1_ls_l: str, local2_ls_l:str,
            exclude_fs: list[str] | None = None,
            p=True, 
            exclude_dirs=None,
            include_fs=None,
            recursive=True,
            force_download=False,
            reverse=False,
            ):
        if exclude_fs is None:
            exclude_fs = []
        if exclude_dirs is None:
            exclude_dirs = []
        local1_files = list(local1_ls_l.keys())
        download_fs = list(local2_ls_l.keys())
        download_fs = [f for f in download_fs if not local2_ls_l[f]["isdir"]]
        download_fs = [f for f in download_fs if not f in exclude_fs]
        if not include_fs is None:
            download_fs = [f for f in download_fs if f in include_fs]
        need_fs = [f for f in download_fs if (not f in local1_files)]
        update_fs = [f for f in download_fs if not f in need_fs]
        if not force_download:
            _update_fs = []
            for f in update_fs:
                local1_time = datetime.fromtimestamp(getatime(opj(local1_dir, f)))
                local1_time = datetime(local1_time.year, local1_time.month, local1_time.day, local1_time.hour, local1_time.minute)
                local2_time = datetime.fromtimestamp(getatime(opj(local2_dir, f)))
                local2_time = datetime(local2_time.year, local2_time.month, local2_time.day, local2_time.hour, local2_time.minute)
                local1_size = os.path.getsize(opj(local1_dir, f))
                local2_size = os.path.getsize(opj(local2_dir, f))
                outdated = local1_time < local2_time
                dif_size = local1_size != int(local2_size)
                if outdated or dif_size:
                    _update_fs.append(f)
            update_fs = _update_fs
        if len(need_fs) or len(update_fs):
            print(f"Updating {local2_dir} --> {local1_dir}")
            print(f"Updating files {update_fs}")
            print(f"Downloading files {need_fs}")
            download_fs = update_fs + need_fs
            for f in download_fs:
                self.download(opj(local2_dir, f), local1_dir, p=p)
        self.write_dir_updated_timestamp(local1_dir)
        if recursive:
            local2_dirs = [f for f in local2_ls_l if local2_ls_l[f]["isdir"]]
            for d in local2_dirs:
                if not d in exclude_dirs:
                    if not reverse:
                        self.update_dir_contents(
                            self.local1.pcat(local1_dir, [d], force=True),
                            self.local2.pcat(local2_dir, [d], force=False),
                            exclude_fs=exclude_fs,
                            exclude_dirs=exclude_dirs,
                            include_fs=include_fs,
                            recursive=recursive,
                            force_download=force_download,
                            p=p,
                            reverse=reverse,
                            )
                    else:
                        self.update_dir_contents(
                            self.local2.pcat(local1_dir, [d], force=True),
                            self.local1.pcat(local2_dir, [d], force=False),
                            exclude_fs=exclude_fs,
                            exclude_dirs=exclude_dirs,
                            include_fs=include_fs,
                            recursive=recursive,
                            force_download=force_download,
                            p=p,
                            reverse=reverse,
                            )
        

    def update_dir_contents(
            self, local1_dir, local2_dir = None,
            exclude_fs: list[str] | None = None,
            p=True, 
            exclude_dirs=None,
            include_fs=None,
            recursive=True,
            force_download=False,
            reverse=False,
            ):
        if exclude_fs is None:
            exclude_fs = []
        if exclude_dirs is None:
            exclude_dirs = []
        if not reverse:
            if local2_dir is None:
                local2_dir = self.get_local2_path(local1_dir)
            if not self.local1.ope(local1_dir):
                self.local1.mkdir(local1_dir)
            local2_ls_l = self.local2.get_ls_l_file_info(local2_dir)
            local1_ls_l = self.local1.get_ls_l_file_info(local1_dir)
        else:
            if local2_dir is None:
                local2_dir = self.get_local1_path(local2_dir)
            if not self.local1.ope(local2_dir):
                self.local1.mkdir(local2_dir)
            local1_ls_l = self.local2.get_ls_l_file_info(local1_dir)
            local2_ls_l = self.local1.get_ls_l_file_info(local2_dir)
        self.update_dir_contents_helper(
            local1_dir, local2_dir, local1_ls_l, local2_ls_l,
            exclude_fs=exclude_fs,
            exclude_dirs=exclude_dirs,
            include_fs=include_fs,
            recursive=recursive,
            force_download=force_download,
            p=p,
            reverse=reverse,
        )

    def get_dir_updated_timestamp(self, local1_path):
        fname = opj(local1_path, "last_updated.txt")
        last_updated = None
        if ope(fname):
            timestamp = ""
            with open(fname, "r") as f:
                for line in f:
                    timestamp = line
            last_updated = float(timestamp.strip())
        return last_updated

        
    def need_check(self, local1_path, day_freq, force_check=False):
        if force_check:
            return True
        else:
            cur = time.time()
            last = self.get_dir_updated_timestamp(self, local1_path)
            if last is None:
                return True
            else:
                elap = cur - last
                df2s = day_freq*24*60*60
                return elap > df2s
            
    

    
        
        
