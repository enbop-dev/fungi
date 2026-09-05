use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    ffi::OsString,
    path::Path,
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};
use sysinfo::{Pid, ProcessStatus, Signal, System};

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub(crate) struct ProcessId {
    pub(crate) pid: u32,
    started_at: u64,
}

impl ProcessId {
    pub(crate) fn capture(pid: u32) -> Result<Self> {
        let system = System::new_all();
        let process = system
            .process(Pid::from_u32(pid))
            .context("spawned process already exited")?;
        Ok(Self {
            pid,
            started_at: start_identity(process)?,
        })
    }

    fn inspect(&self, exe: &Path, args: &[OsString]) -> Result<Option<System>> {
        let system = System::new_all();
        let Some(process) = system.process(Pid::from_u32(self.pid)) else {
            return Ok(None);
        };
        if process.status() == ProcessStatus::Zombie {
            return Ok(None);
        }
        // A reused PID, different executable, or changed command is not ours.
        let mut replaced_exe = exe.as_os_str().to_os_string();
        replaced_exe.push(" (deleted)");
        let executable_matches = process.exe() == Some(exe)
            || (cfg!(target_os = "linux") && process.exe() == Some(Path::new(&replaced_exe)));
        // Rebuilding a binary unlinks the old image on Linux. PID/start time
        // and exact arguments still identify that same running lab process.
        let started_at = start_identity(process)?;
        if started_at != self.started_at
            || !executable_matches
            || !process.cmd().iter().skip(1).eq(args.iter())
        {
            bail!(
                "refusing pid {}: identity mismatch (start {} vs {}, executable match: {}, arguments match: {})",
                self.pid,
                self.started_at,
                started_at,
                executable_matches,
                process.cmd().iter().skip(1).eq(args.iter())
            );
        }
        Ok(Some(system))
    }

    pub(crate) fn running(&self, exe: &Path, args: &[OsString]) -> Result<bool> {
        Ok(self.inspect(exe, args)?.is_some())
    }

    pub(crate) fn stop(&self, exe: &Path, args: &[OsString]) -> Result<()> {
        for signal in [Signal::Term, Signal::Kill] {
            let Some(system) = self.inspect(exe, args)? else {
                return Ok(());
            };
            let process = system.process(Pid::from_u32(self.pid)).unwrap();
            if !process.kill_with(signal).unwrap_or_else(|| process.kill()) {
                bail!("failed to signal lab pid {}", self.pid);
            }
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                if !self.running(exe, args)? {
                    return Ok(());
                }
                thread::sleep(Duration::from_millis(100));
            }
        }
        bail!("lab pid {} did not stop", self.pid)
    }
}

fn start_identity(process: &sysinfo::Process) -> Result<u64> {
    #[cfg(target_os = "linux")]
    {
        // Use the kernel's start ticks, not an estimated wall-clock boot time
        // which can shift after a clock adjustment. stat field 22 follows comm.
        let stat = std::fs::read_to_string(format!("/proc/{}/stat", process.pid()))?;
        let fields = stat.rsplit_once(')').context("invalid process stat")?.1;
        Ok(fields
            .split_whitespace()
            .nth(19)
            .context("missing process start ticks")?
            .parse()?)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(process.start_time())
    }
}

/// Own a child until startup succeeds. Errors and unwinding reclaim only this child.
pub(crate) struct ChildGuard(pub(crate) Option<Child>);

impl ChildGuard {
    pub(crate) fn spawn(command: &mut Command) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        Ok(Self(Some(
            command.spawn().context("failed to spawn lab process")?,
        )))
    }

    pub(crate) fn child(&mut self) -> &mut Child {
        self.0.as_mut().unwrap()
    }

    pub(crate) fn ensure_running(&mut self) -> Result<()> {
        if let Some(status) = self.child().try_wait()? {
            bail!("process exited before startup completed ({status})");
        }
        Ok(())
    }

    pub(crate) fn stop(&mut self) -> Result<()> {
        if let Some(child) = self.0.as_mut() {
            if child.try_wait()?.is_none() {
                child.kill()?;
            }
            child.wait()?;
        }
        self.0 = None;
        Ok(())
    }

    pub(crate) fn release(mut self) {
        self.0 = None;
    }
}
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            eprintln!("failed to reclaim child: {error:#}");
        }
    }
}
