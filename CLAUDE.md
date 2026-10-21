# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

`remotePathSync` mirrors a directory tree between a local machine and a remote HPC cluster over SSH, and submits/tracks SLURM jobs for "calculation" directories in that tree. It is used interactively (Jupyter, see [examples/](examples/)), so most methods print progress instead of logging.

There is no test suite, linter config, or build tooling. It is installed editable (`pip install -e .`) into the `research` conda env (`~/miniforge3/envs/research`, Python 3.12), so edits here take effect there immediately. Requires Python >= 3.10.

## Downstream usage (`../research`)

The main consumer is the sibling repo `/Users/richb/vs/research`. Check it before changing public method signatures or default behavior.
- [src/research/io/remote_io_pathlib.py](../research/src/research/io/remote_io_pathlib.py) has the user's `PathRootPair` subclass. It holds the per-cluster config (perlmutter, alpine, bridges/bridges-shared with `data.bridges2.psc.edu` transfer hosts, anvil with a key file). It also overrides `__post_init__` (per-cluster `slurm_file_name` and `exclude_fs_default`), `zip_transfer`, `zip_transfer_multi`, and `upload` positionally, so it never uploads electron-density files (`n_up`, `n_dn`). Changing those methods' signatures or argument order will break it.
- Typical use is a script such as `NO3_ads_gen/gen/from_aziz/sub_missing_molecules.py`: `PathRootPair.from_paths(local_root=..., remote_root=..., cluster="perlmutter", proc_alias="perlmutter")`, then `psu.upload_dir([calc_root])`, `psu.submit_path_psubmit(calc_root, slurm_file_name=..., use_proc=True, check_state=False)`, `psu.remote_ope(...)`, `psu.download_dir(list_of_paths)`, and `psu.remote.run("squeue ...")`. One-item lists are common, and they go through `zip_transfer_multi`/`make_zip_multi`, not `zip_transfer`.
- For a live check, `from_paths(local_root=<tmp dir>, cluster="anvil", username="x-brich")` connects non-interactively with a key. Work in a throwaway subdirectory of `/anvil/scratch/x-brich`, delete it afterwards, and don't submit jobs.
- For an offline check of upload/download, build the pair from two local `PathRoot`s (`hostname=None`) and give the "remote" one a stub `scp` with `put(src, dst_dir)`/`get(src, dst_dir)` that copies files.

## Architecture

There are two modules under [src/remotePathSync/](src/remotePathSync/):

**`PathRoot`** ([pathroot.py](src/remotePathSync/pathroot.py)) represents one filesystem root, local (`hostname=None`) or remote. Methods like `run`, `ope`, `isdir`, `mkdir`, `rm`, `mv`, `make_zip`, and `unzip` branch on `self.remote`. Remote mode uses a paramiko SSH client plus SCP and a lazily opened SFTP client. A few points that span the file:
- Authentication order: `key_filename` (agent and key search disabled), then SSH agent keys (newest first), then an interactive password+OTP prompt, which is Fernet-encrypted in memory.
- `transfer_pathroot`: some clusters (e.g. Bridges-2) use separate login and data-transfer hosts. When set, SFTP goes through the transfer host, and `PathRootPair.download/upload` use its SCP.
- `proc_run` keeps one long-lived `ssh <proc_alias>` subprocess open (this relies on the user's `~/.ssh/config` alias) and finds where each command's output ends with a sentinel echo. Directory listings (`get_ls_fs`, `get_ls_l_file_info`) go through it because parsing `ls` output is much faster than `sftp.listdir`. With `proc_okay=False` it falls back to `run` (paramiko `exec_command`).
- `rm`, `mv`, and `mkdir` refuse paths outside `self.root`.

**`PathRootPair`** ([pathrootpair.py](src/remotePathSync/pathrootpair.py)) pairs a local and a remote `PathRoot` with the same relative layout:
- Path mapping: most public methods take an "arb path" (a local *or* remote path). `get_local_remote_from_arb` resolves it to a `(local, remote)` pair by checking which root it falls under. Keep accepting either kind of path in new methods.
- Configuration by subclassing: users subclass `PathRootPair` and set class attributes (`username`/`usernames`, `hostnames`, `transfer_hostnames`, `local_roots`, `remote_roots`, `key_filenames`, `proc_aliases`) keyed by a cluster name, then call `from_paths(cluster=...)`. Explicit arguments override the class attributes. `__post_init__` is a hook for subclasses, and it runs twice under `from_paths` (once before `self.cluster` is set).
- Transfers: `download_dir`/`upload_dir` default to `as_zip=True`. This zips on the source side, copies one file, unzips on the destination, and deletes both zips. Files named in `exclude_fs_default` (large DFT outputs such as `wfns` and `fluidState`) are skipped by default. Filters become `zip -x/-i '*/<name>'` patterns, so every zip entry must have a directory prefix. With `as_zip=False`, downloads go through `update_dir_contents`, which compares times and sizes from `ls -l` and writes `last_updated.txt` into each local dir.
- SLURM: `collect_slurm_jobs_history` runs `sacct --parsable2` with the fields in `SACCT_FIELDS` and groups jobs by `WorkDir` (the remote calc root). When `smart_job_sort` is on, jobs are ordered by their latest state-change time, with RUNNING forced last. The result is cached in `job_cache[str(days)]` for `job_cache_refresh_time` seconds. `recent_submission` records calc roots submitted since the last cache refresh, and it blocks resubmission until `update_job_cache` clears it. Submission state rules (RUNNING, PENDING, COMPLETED, FAILED, CANCELLED, TIMEOUT, and which flags override each one) are in `_submit_local_path_handle_job_state`. The submit command comes from `submit_command_template` / `slurm_file_name` (`sbatch psubmit.sh` by default).

`LocalPathRootPair` (local↔local) is marked in-source as possibly broken and unmaintained.

## Conventions

- Code style uses `not x is None`, long one-line signatures, and short `""" ... """` docstrings. Keep comments sparse and explain *why*.
- Shell commands built from paths should use `shlex.quote` (newer code does; older `ls`/`zip` calls use manual single quotes).
