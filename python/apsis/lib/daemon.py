import logging
import os
import resource

log = logging.getLogger("daemon")

# -------------------------------------------------------------------------------


def get_max_fds():
    _, limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    return 2048 if limit == resource.RLIM_INFINITY else limit


def close_fds(keep_fds):
    keep_fds = list(sorted(keep_fds))
    keep_fds.append(get_max_fds())
    fd0 = 0
    for fd1 in keep_fds:
        if fd0 < fd1:
            os.closerange(fd0, fd1)
        fd0 = fd1 + 1


def daemonize(log_path, *, keep_fds=[]):
    # Redirect stdin from /dev/null.
    null_fd = os.open("/dev/null", os.O_RDONLY)
    os.dup2(null_fd, 0)
    os.close(null_fd)

    # Redirect stdout/stderr to a log file.
    logging.debug(f"redirecting logs: {log_path}")
    log_fd = os.open(log_path, os.O_CREAT | os.O_APPEND | os.O_WRONLY)
    os.dup2(log_fd, 1)
    os.dup2(log_fd, 2)
    os.close(log_fd)

    close_fds([0, 1, 2] + list(keep_fds))

    # Double-fork to detach.
    log.info("detaching")
    if os.fork() > 0:
        os._exit(0)
    os.setsid()
    if os.fork() > 0:
        os._exit(0)
