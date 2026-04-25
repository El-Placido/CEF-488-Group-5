#include <stdio.h>      // Standard input/output functions
#include <stdlib.h>     // Standard library functions like exit()
#include <unistd.h>     // POSIX API: fork(), chdir(), close(), sleep(), usleep()
#include <fcntl.h>      // File control options: open(), fcntl()
#include <signal.h>     // Signal handling: sigaction(), SIGTERM, SIGINT, SIGCHLD
#include <sys/types.h>  // System data types
#include <sys/stat.h>   // File mode constants and umask()
#include <string.h>     // String handling: strerror()
#include <errno.h>      // Error reporting with errno
#include <sys/wait.h>   // Process waiting: waitpid()
#include <sys/resource.h> // Resource limits: setrlimit()
#include <sys/file.h>   // File locking: flock()

// Global flag used to control the main daemon loop.
// sig_atomic_t is used because it is safe to modify in a signal handler.
volatile sig_atomic_t running = 1;

// Stores the PID of the child process.
pid_t child_pid;

// Two unnamed pipes for bidirectional communication:
// pipe1: parent writes -> child reads
// pipe2: child writes -> parent reads
int pipe1[2], pipe2[2];

// Log file pointer used by the daemon to store messages.
FILE *logfile;

/*
 Signal handler function which handles SIGTERM/SIGINT and SIGCHLD
 */
void handle_signal(int sig) {
    if (sig == SIGTERM || sig == SIGINT) {
        // Stop the daemon loop when termination or interrupt signal is received
        running = 0;
        fprintf(logfile, "Daemon shutting down (signal %d)\n", sig);
        fflush(logfile);

    } else if (sig == SIGCHLD) {
        // Reap any finished child processes without blocking
        while (waitpid(-1, NULL, WNOHANG) > 0);
    }
}

//Daemonization

void daemonize() {
    pid_t pid = fork();

    // If fork fails, terminate immediately
    if (pid < 0) exit(EXIT_FAILURE);

    // Parent exits so child can continue in background
    if (pid > 0) exit(EXIT_SUCCESS);

    // Create a new session and detach from controlling terminal
    setsid();

    // Change working directory to root so the daemon does not block filesystem unmounts
    chdir("/");

    // Reset file creation mask
    umask(0);

    // Close all inherited file descriptors
    for (int fd = 0; fd < sysconf(_SC_OPEN_MAX); fd++) close(fd);

    // Open a custom log file for daemon output
    logfile = fopen("/tmp/mydaemon.log", "a+");
    if (!logfile) exit(EXIT_FAILURE);
}

int main() {
    // Turn this program into a daemon
    daemonize();

    // Create/open PID file and use flock() to prevent multiple instances
    int pid_fd = open("/tmp/mydaemon.pid", O_RDWR | O_CREAT, 0640);
    if (pid_fd < 0) {
        fprintf(logfile, "Cannot open PID file\n");
        exit(EXIT_FAILURE);
    }

    // Try to lock the PID file
    // If lock fails, another instance is probably running
    if (flock(pid_fd, LOCK_EX | LOCK_NB) < 0) {
        fprintf(logfile, "Daemon already running\n");
        exit(EXIT_FAILURE);
    }

    // Write current daemon PID to the PID file
    dprintf(pid_fd, "%d\n", getpid());

    // Set up signal handling using sigaction()
    struct sigaction sa;
    sa.sa_handler = handle_signal;   // Function to call when signal is received
    sigemptyset(&sa.sa_mask);        // No additional signals blocked during handler
    sa.sa_flags = SA_RESTART;        // Restart interrupted system calls where possible

    // Register handler for termination, interrupt, and child exit signals
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGCHLD, &sa, NULL);

    // Simulate resource exhaustion by limiting number of processes
    struct rlimit rl;
    rl.rlim_cur = 1;
    rl.rlim_max = 1;
    setrlimit(RLIMIT_NPROC, &rl);

    // Create two unnamed pipes for parent-child communication
    if (pipe(pipe1) < 0 || pipe(pipe2) < 0) {
        fprintf(logfile, "Pipe creation failed\n");
        exit(EXIT_FAILURE);
    }

    // Fork child process
    child_pid = fork();
    if (child_pid < 0) {
        fprintf(logfile, "Fork failed: %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    if (child_pid == 0) {
     
        // Close unused ends of the pipes
        close(pipe1[1]); // child does not write to pipe1
        close(pipe2[0]); // child does not read from pipe2

        // Set read end of pipe1 to non-blocking mode
        fcntl(pipe1[0], F_SETFL, O_NONBLOCK);

        int sum = 0, num;

        while (1) {
            // Attempt to read an integer from parent
            ssize_t r = read(pipe1[0], &num, sizeof(int));

            if (r > 0) {
                // Successfully received a number
                sum += num;

                // Send updated running sum back to parent
                write(pipe2[1], &sum, sizeof(int));

            } else if (r < 0 && errno == EAGAIN) {
                // No data available right now; do other work
                usleep(100000);

            } else if (r == 0) {
                // Pipe closed by parent, so exit loop
                break;
            }
        }

        // Exit child cleanly
        exit(EXIT_SUCCESS);

    } else {

//Parent daemon section
//simulating fork
/*struct rlimit rl;
rl.rlim_cur = 1;
rl.rlim_max = 1;
setrlimit(RLIMIT_NPROC, &rl);
*/
pid_t test_pid = fork();
if (test_pid < 0) {
    fprintf(logfile, "Simulated fork failure: %s\n", strerror(errno));
    fflush(logfile);
} else if (test_pid == 0) {
    exit(EXIT_SUCCESS);
} else {
    waitpid(test_pid, NULL, 0);
}
        // Close unused ends of the pipes
        close(pipe1[0]); // parent does not read from pipe1
        close(pipe2[1]); // parent does not write to pipe2

        // Set parent write end and read end to non-blocking mode
        fcntl(pipe1[1], F_SETFL, O_NONBLOCK);
        fcntl(pipe2[0], F_SETFL, O_NONBLOCK);

        int num = 1, result;

        // Main daemon loop runs until a termination signal changes running to 0
        while (running) {
            // Send next integer to child
            write(pipe1[1], &num, sizeof(int));
            num++;

            // Simulate periodic daemon work
            sleep(1);

            // Try to read result from child
            ssize_t r = read(pipe2[0], &result, sizeof(int));
            if (r > 0) {
                // Log received running sum
                fprintf(logfile, "Received sum: %d\n", result);
                fflush(logfile);
            }
        }

//Cleanup section
         
        // Tell child to terminate
        kill(child_pid, SIGTERM);

        // Wait for child process to exit
        waitpid(child_pid, NULL, 0);

        // Remove PID file
        unlink("/tmp/mydaemon.pid");

        // Close log file
        fclose(logfile);

        // Exit daemon successfully
        exit(EXIT_SUCCESS);
    }
}