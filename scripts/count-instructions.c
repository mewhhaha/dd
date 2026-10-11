// Count user-mode instructions in a command and all its threads on Linux.
#define _GNU_SOURCE
#include <errno.h>
#include <linux/perf_event.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: count-instructions COMMAND [ARG...]\n");
        return 2;
    }
    struct perf_event_attr event = {
        .type = PERF_TYPE_HARDWARE,
        .size = sizeof(event),
        .config = PERF_COUNT_HW_INSTRUCTIONS,
        .disabled = 1,
        .inherit = 1,
        .exclude_kernel = 1,
        .exclude_hv = 1,
        .enable_on_exec = 1,
        .read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING,
    };
    int counter = syscall(SYS_perf_event_open, &event, 0, -1, -1, 0);
    if (counter < 0) {
        perror("perf_event_open");
        return 1;
    }
    pid_t child = fork();
    if (child < 0) {
        perror("fork");
        return 1;
    }
    if (child == 0) {
        close(counter);
        execvp(argv[1], argv + 1);
        perror("execvp");
        _exit(127);
    }
    int status;
    while (waitpid(child, &status, 0) < 0) {
        if (errno != EINTR) {
            perror("waitpid");
            return 1;
        }
    }
    uint64_t counts[3];
    if (read(counter, counts, sizeof(counts)) != sizeof(counts)) {
        perror("read counter");
        return 1;
    }
    close(counter);
    fprintf(stderr,
            "{\"instructions\":%llu,\"time_enabled\":%llu,\"time_running\":%llu}\n",
            (unsigned long long)counts[0], (unsigned long long)counts[1],
            (unsigned long long)counts[2]);
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    return 128 + WTERMSIG(status);
}
