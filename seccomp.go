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

	syscalls := []specs.LinuxSyscall{
		allow(
			// process / thread lifecycle
			"execve", "execveat",
			"exit", "exit_group",
			"fork", "vfork",
			"wait4", "waitid",
			"getpid", "getppid", "gettid",
			"set_tid_address", "set_robust_list", "get_robust_list",
			"prctl", "arch_prctl",
			"capget",
			"getrandom",
			"rseq",

			// signals
			"kill", "tgkill", "tkill",
			"rt_sigaction", "rt_sigprocmask", "rt_sigreturn",
			"rt_sigpending", "rt_sigsuspend", "rt_sigtimedwait",
			"rt_sigqueueinfo", "rt_tgsigqueueinfo",
			"sigaltstack",
			"pidfd_open", "pidfd_send_signal",
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
			"shutdown",
			"getsockname", "getpeername",
			"getsockopt", "setsockopt",
			"socketpair",

			// ioctl is unavoidable — used for terminal sizing, etc.
			"ioctl",
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
	}

	return &specs.LinuxSeccomp{
		DefaultAction: specs.ActErrno,
		Architectures: []specs.Arch{specs.ArchX86_64},
		Syscalls:      syscalls,
	}
}
