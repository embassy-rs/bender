package main

import (
	"context"

	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
)

// withSeccomp installs a minimal seccomp profile tailored for plain CI jobs
// (git, cargo, rustc and friends). It is a stripped-down fork of containerd's
// default profile with everything cargo/rustc don't need removed, plus tighter
// argument filters on `socket` and `clone`.
//
// Only the native x86_64 ABI is permitted; x86 and x32 syscalls return ENOSYS,
// which closes the "use the 32-bit ABI to bypass the filter" escape route.
func withSeccomp() oci.SpecOpts {
	return func(_ context.Context, _ oci.Client, _ *containers.Container, s *specs.Spec) error {
		s.Linux.Seccomp = ciSeccompProfile()
		return nil
	}
}

func ciSeccompProfile() *specs.LinuxSeccomp {
	nosys := uint(unix.ENOSYS)

	allow := func(names ...string) specs.LinuxSyscall {
		return specs.LinuxSyscall{
			Names:  names,
			Action: specs.ActAllow,
		}
	}

	prctlAllow := func(op int) specs.LinuxSyscall {
		return specs.LinuxSyscall{
			Names:  []string{"prctl"},
			Action: specs.ActAllow,
			Args: []specs.LinuxSeccompArg{{
				Index: 0,
				Value: uint64(op),
				Op:    specs.OpEqualTo,
			}},
		}
	}

	ioctlAllow := func(cmd uint64) specs.LinuxSyscall {
		return specs.LinuxSyscall{
			Names:  []string{"ioctl"},
			Action: specs.ActAllow,
			Args: []specs.LinuxSeccompArg{{
				Index: 1,
				Value: cmd,
				Op:    specs.OpEqualTo,
			}},
		}
	}

	syscalls := []specs.LinuxSyscall{
		allow(
			// process / thread lifecycle
			"execve", "execveat",
			"exit", "exit_group",
			"fork", "vfork",
			"wait4", "waitid",
			"getpid", "getppid", "gettid",
			"set_tid_address", "set_robust_list", "get_robust_list",
			"arch_prctl",
			"capget",
			"getrandom",
			"rseq",

			// signals
			"kill", "tgkill", "tkill",
			"rt_sigaction", "rt_sigprocmask", "rt_sigreturn",
			"rt_sigpending", "rt_sigsuspend", "rt_sigtimedwait",
			"rt_sigqueueinfo", "rt_tgsigqueueinfo",
			"sigaltstack",
			"pidfd_send_signal",
			"restart_syscall",
			"signalfd4",

			// scheduling (just what rayon/tokio need)
			"sched_yield",
			"sched_getaffinity", "sched_setaffinity",
			"getpriority", "setpriority",

			// time
			"clock_gettime", "clock_getres", "clock_nanosleep",
			"nanosleep",
			"gettimeofday",
			"getitimer", "setitimer",
			"timerfd_create", "timerfd_gettime", "timerfd_settime",

			// futexes
			"futex", "futex_waitv",

			// memory
			"brk", "mmap", "mprotect", "munmap", "mremap", "madvise",
			"membarrier", "memfd_create",
			"remap_file_pages", "msync",

			// file I/O
			"open", "openat", "openat2", "creat",
			"close", "close_range",
			"read", "readv", "pread64", "preadv", "preadv2",
			"write", "writev", "pwrite64", "pwritev", "pwritev2",
			"lseek",
			"fsync", "fdatasync", "sync", "syncfs", "sync_file_range",
			"fallocate", "ftruncate", "truncate",
			"copy_file_range", "sendfile", "splice",
			"fadvise64",
			"readahead",

			// metadata
			"stat", "lstat", "fstat", "newfstatat", "statx", "statfs", "fstatfs",
			"access", "faccessat", "faccessat2",
			"readlink", "readlinkat",
			"getcwd", "chdir", "fchdir",
			"umask",

			// dir / name manipulation
			"getdents64",
			"mkdir", "mkdirat", "rmdir",
			"rename", "renameat", "renameat2",
			"link", "linkat", "symlink", "symlinkat",
			"unlink", "unlinkat",

			// permissions
			"chmod", "fchmod", "fchmodat",
			"chown", "fchown", "lchown", "fchownat",
			"utime", "utimes", "utimensat",

			// xattrs (git/tar/cargo unpack)
			"getxattr", "lgetxattr", "fgetxattr",
			"setxattr", "lsetxattr", "fsetxattr",
			"listxattr", "llistxattr", "flistxattr",
			"removexattr", "lremovexattr", "fremovexattr",

			// fds
			"dup", "dup2", "dup3",
			"fcntl",
			"pipe", "pipe2",
			"eventfd2",
			"poll", "ppoll",
			"select", "pselect6",
			"epoll_create1", "epoll_ctl", "epoll_pwait", "epoll_wait",
			"flock",

			// identity
			"getuid", "geteuid", "getgid", "getegid", "getgroups",
			"getresuid", "getresgid",
			"getsid", "getpgid", "getpgrp", "setpgid", "setsid",

			// limits / info
			"getrlimit", "setrlimit", "prlimit64",
			"getrusage",
			"uname", "sysinfo", "getcpu",

			// network (sockets — ranges and address families filtered below)
			"connect", "bind", "listen", "accept", "accept4",
			"sendto", "recvfrom", "sendmsg", "recvmsg",
			"sendmmsg", "recvmmsg",
			"shutdown",
			"getsockname", "getpeername",
			"getsockopt",
			"socketpair",
		),

		// clone, but only without new namespaces (no unshare-via-clone)
		{
			Names:  []string{"clone"},
			Action: specs.ActAllow,
			Args: []specs.LinuxSeccompArg{{
				Index:    0,
				Value:    unix.CLONE_NEWNS | unix.CLONE_NEWUTS | unix.CLONE_NEWIPC | unix.CLONE_NEWUSER | unix.CLONE_NEWPID | unix.CLONE_NEWNET | unix.CLONE_NEWCGROUP,
				ValueTwo: 0,
				Op:       specs.OpMaskedEqual,
			}},
		},
		// clone3 has a struct arg that seccomp can't introspect — return
		// ENOSYS so glibc falls back to clone (which we filter above).
		{
			Names:    []string{"clone3"},
			Action:   specs.ActErrno,
			ErrnoRet: &nosys,
		},

		// Deny SOCK_RAW (and SOCK_PACKET=10) before the per-family allows
		// below. Cargo/git never open raw sockets. Mask 0xF strips the
		// SOCK_CLOEXEC / SOCK_NONBLOCK flag bits.
		{
			Names:  []string{"socket"},
			Action: specs.ActErrno,
			Args: []specs.LinuxSeccompArg{{
				Index:    1,
				Value:    0xF,
				ValueTwo: unix.SOCK_RAW,
				Op:       specs.OpMaskedEqual,
			}},
		},
		{
			Names:  []string{"socket"},
			Action: specs.ActErrno,
			Args: []specs.LinuxSeccompArg{{
				Index:    1,
				Value:    0xF,
				ValueTwo: 10, // SOCK_PACKET (obsolete but kernel still accepts it)
				Op:       specs.OpMaskedEqual,
			}},
		},
		// socket, restricted to common address families. AF_INET/INET6 for
		// HTTPS to crates.io/github (gated further by the bender net sandbox),
		// AF_UNIX for local IPC, AF_NETLINK for getaddrinfo.
		{
			Names:  []string{"socket"},
			Action: specs.ActAllow,
			Args: []specs.LinuxSeccompArg{{
				Index: 0,
				Value: unix.AF_UNIX,
				Op:    specs.OpEqualTo,
			}},
		},
		{
			Names:  []string{"socket"},
			Action: specs.ActAllow,
			Args: []specs.LinuxSeccompArg{{
				Index: 0,
				Value: unix.AF_INET,
				Op:    specs.OpEqualTo,
			}},
		},
		{
			Names:  []string{"socket"},
			Action: specs.ActAllow,
			Args: []specs.LinuxSeccompArg{{
				Index: 0,
				Value: unix.AF_INET6,
				Op:    specs.OpEqualTo,
			}},
		},
		{
			Names:  []string{"socket"},
			Action: specs.ActAllow,
			Args: []specs.LinuxSeccompArg{{
				Index: 0,
				Value: unix.AF_NETLINK,
				Op:    specs.OpEqualTo,
			}},
		},

		// prctl: allowlist the handful of ops glibc + cargo/git need.
		// Block PR_SET_MM (LPE historically), PR_SET_SECCOMP (could install
		// a permissive filter; no-op under NNP but block anyway),
		// PR_SET_SPECULATION_CTRL, PR_SET_SECUREBITS, PR_SCHED_CORE, etc.
		prctlAllow(unix.PR_SET_PDEATHSIG),
		prctlAllow(unix.PR_GET_PDEATHSIG),
		prctlAllow(unix.PR_GET_DUMPABLE),
		prctlAllow(unix.PR_SET_DUMPABLE),
		prctlAllow(unix.PR_GET_KEEPCAPS),
		prctlAllow(unix.PR_SET_KEEPCAPS),
		prctlAllow(unix.PR_GET_NAME),
		prctlAllow(unix.PR_SET_NAME),
		prctlAllow(unix.PR_GET_NO_NEW_PRIVS),
		prctlAllow(unix.PR_SET_NO_NEW_PRIVS),
		prctlAllow(unix.PR_CAP_AMBIENT),

		// setsockopt: deny netlink and packet sockets' setsockopt entirely.
		// These have been the source of multiple kernel heap-overflow CVEs
		// (CVE-2021-22555, CVE-2022-25636, etc.). Git/cargo only setsockopt
		// on TCP/UDP/UNIX sockets.
		{
			Names:  []string{"setsockopt"},
			Action: specs.ActErrno,
			Args: []specs.LinuxSeccompArg{{
				Index: 1,
				Value: unix.SOL_NETLINK,
				Op:    specs.OpEqualTo,
			}},
		},
		{
			Names:  []string{"setsockopt"},
			Action: specs.ActErrno,
			Args: []specs.LinuxSeccompArg{{
				Index: 1,
				Value: unix.SOL_PACKET,
				Op:    specs.OpEqualTo,
			}},
		},
		allow("setsockopt"),

		// ioctl: explicit allowlist of commands actually used by CI tooling.
		// Anything not listed falls through to the default-deny. Goal is
		// defense against future, not-yet-known ioctl-reachable kernel bugs.
		//
		// Notable cmds intentionally *not* allowed: TIOCSTI (TTY injection),
		// TIOCLINUX, TIOCSETD (line-discipline swap), TIOCCONS, VT_*, KD*,
		// BLK*, LOOP_*, DM_*, NS_GET_*, BTRFS_IOC_*, SIOCS*. If a build
		// trips on EPERM from ioctl, strace will name the cmd; add it here.

		/*
			// termios — get/set terminal attributes (every readline/curses user)
			ioctlAllow(0x5401), // TCGETS
			ioctlAllow(0x5402), // TCSETS
			ioctlAllow(0x5403), // TCSETSW
			ioctlAllow(0x5404), // TCSETSF
			ioctlAllow(0x5405), // TCGETA
			ioctlAllow(0x5406), // TCSETA
			ioctlAllow(0x5407), // TCSETAW
			ioctlAllow(0x5408), // TCSETAF
			ioctlAllow(0x5409), // TCSBRK
			ioctlAllow(0x540A), // TCXONC
			ioctlAllow(0x540B), // TCFLSH

			// TTY misc — window size, session, fg pgrp, pty allocation
			ioctlAllow(0x540C), // TIOCEXCL
			ioctlAllow(0x540D), // TIOCNXCL
			ioctlAllow(0x540E), // TIOCSCTTY
			ioctlAllow(0x540F), // TIOCGPGRP
			ioctlAllow(0x5410), // TIOCSPGRP
			ioctlAllow(0x5411), // TIOCOUTQ
			ioctlAllow(0x5413), // TIOCGWINSZ
			ioctlAllow(0x5414), // TIOCSWINSZ
			ioctlAllow(0x5422), // TIOCNOTTY
			ioctlAllow(0x5424), // TIOCGETD     (note: SETD intentionally not allowed)
			ioctlAllow(0x5429), // TIOCGSID
			ioctlAllow(0x80045430), // TIOCGPTN
			ioctlAllow(0x40045431), // TIOCSPTLCK
			ioctlAllow(0x5441),     // TIOCGPTPEER
		*/

		// FD/file generic — glibc & stdlib internals
		ioctlAllow(0x541B), // FIONREAD / TIOCINQ
		ioctlAllow(0x5421), // FIONBIO
		ioctlAllow(0x5450), // FIONCLEX
		ioctlAllow(0x5451), // FIOCLEX
		ioctlAllow(0x5452), // FIOASYNC

		// filesystem — reflink, chattr, fiemap (cargo, cp --reflink, tar)
		ioctlAllow(0x40049409), // FICLONE
		ioctlAllow(0x4020940D), // FICLONERANGE
		ioctlAllow(0xC0189436), // FIDEDUPERANGE
		ioctlAllow(0x80086601), // FS_IOC_GETFLAGS
		ioctlAllow(0x40086602), // FS_IOC_SETFLAGS
		ioctlAllow(0x80087601), // FS_IOC_GETVERSION
		ioctlAllow(0x40087602), // FS_IOC_SETVERSION
		ioctlAllow(0xC020660B), // FS_IOC_FIEMAP
		ioctlAllow(0x801C581F), // FS_IOC_FSGETXATTR
		ioctlAllow(0x401C581E), // FS_IOC_FSSETXATTR
	}

	return &specs.LinuxSeccomp{
		DefaultAction: specs.ActErrno,
		Architectures: []specs.Arch{specs.ArchX86_64},
		Syscalls:      syscalls,
	}
}
