#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

struct relay_connection {
    int client_fd;
    int upstream_fd;
};

static volatile sig_atomic_t listener_fd = -1;
static const char *unix_socket_path = NULL;

static void stop_listener(int signal_number) {
    (void)signal_number;
    if (listener_fd >= 0) {
        close(listener_fd);
        listener_fd = -1;
    }
}

static bool parse_port(const char *value, uint16_t *port) {
    char *end = NULL;
    errno = 0;
    long parsed = strtol(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed < 1 || parsed > 65535) {
        return false;
    }
    *port = (uint16_t)parsed;
    return true;
}

static int connect_upstream(const char *target_address, uint16_t target_port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_port = htons(target_port);
    if (inet_pton(AF_INET, target_address, &address.sin_addr) != 1
        || connect(fd, (const struct sockaddr *)&address, sizeof(address)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static bool write_all(int fd, const uint8_t *bytes, size_t count) {
    size_t offset = 0;
    while (offset < count) {
        ssize_t written = write(fd, bytes + offset, count - offset);
        if (written > 0) {
            offset += (size_t)written;
        } else if (written < 0 && errno == EINTR) {
            continue;
        } else {
            return false;
        }
    }
    return true;
}

static void close_input_direction(int input_fd, int output_fd, bool *is_open) {
    if (*is_open) {
        shutdown(input_fd, SHUT_RD);
        shutdown(output_fd, SHUT_WR);
        *is_open = false;
    }
}

static void *relay_connection_main(void *raw_connection) {
    struct relay_connection connection = *(struct relay_connection *)raw_connection;
    free(raw_connection);

    bool client_input_open = true;
    bool upstream_input_open = true;
    uint8_t buffer[64 * 1024];

    while (client_input_open || upstream_input_open) {
        struct pollfd descriptors[2] = {
            {
                .fd = connection.client_fd,
                .events = client_input_open ? POLLIN : 0,
                .revents = 0,
            },
            {
                .fd = connection.upstream_fd,
                .events = upstream_input_open ? POLLIN : 0,
                .revents = 0,
            },
        };

        int result;
        do {
            result = poll(descriptors, 2, -1);
        } while (result < 0 && errno == EINTR);
        if (result < 0) {
            break;
        }

        if (client_input_open && (descriptors[0].revents & (POLLIN | POLLHUP | POLLERR | POLLNVAL))) {
            ssize_t count;
            do {
                count = read(connection.client_fd, buffer, sizeof(buffer));
            } while (count < 0 && errno == EINTR);
            if (count > 0) {
                if (!write_all(connection.upstream_fd, buffer, (size_t)count)) {
                    close_input_direction(connection.client_fd, connection.upstream_fd, &client_input_open);
                }
            } else {
                close_input_direction(connection.client_fd, connection.upstream_fd, &client_input_open);
            }
        }

        if (upstream_input_open && (descriptors[1].revents & (POLLIN | POLLHUP | POLLERR | POLLNVAL))) {
            ssize_t count;
            do {
                count = read(connection.upstream_fd, buffer, sizeof(buffer));
            } while (count < 0 && errno == EINTR);
            if (count > 0) {
                if (!write_all(connection.client_fd, buffer, (size_t)count)) {
                    close_input_direction(connection.upstream_fd, connection.client_fd, &upstream_input_open);
                }
            } else {
                close_input_direction(connection.upstream_fd, connection.client_fd, &upstream_input_open);
            }
        }
    }

    close(connection.client_fd);
    close(connection.upstream_fd);
    return NULL;
}

static int create_tcp_listener(uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    int enabled = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) != 0) {
        close(fd);
        return -1;
    }

    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(fd, (const struct sockaddr *)&address, sizeof(address)) != 0
        || listen(fd, 128) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int create_unix_listener(const char *path) {
    if (strlen(path) >= sizeof(((struct sockaddr_un *)0)->sun_path)) {
        errno = ENAMETOOLONG;
        return -1;
    }

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    struct sockaddr_un address = {0};
    address.sun_family = AF_UNIX;
    snprintf(address.sun_path, sizeof(address.sun_path), "%s", path);
    unlink(path);
    if (bind(fd, (const struct sockaddr *)&address, sizeof(address)) != 0
        || chmod(path, S_IRUSR | S_IWUSR) != 0
        || listen(fd, 128) != 0) {
        close(fd);
        unlink(path);
        return -1;
    }
    return fd;
}

static int run_relay(
    int listening_fd,
    const char *mode,
    const char *listen_endpoint,
    const char *target_address,
    uint16_t target_port
) {
    listener_fd = listening_fd;
    fprintf(
        stderr,
        "ready mode=%s listen=%s target=%s:%u\n",
        mode,
        listen_endpoint,
        target_address,
        target_port
    );
    fflush(stderr);

    while (listener_fd >= 0) {
        int client_fd = accept(listener_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (listener_fd < 0 || errno == EBADF) {
                break;
            }
            fprintf(stderr, "accept failed: %s\n", strerror(errno));
            continue;
        }

        int upstream_fd = connect_upstream(target_address, target_port);
        if (upstream_fd < 0) {
            fprintf(stderr, "upstream connection failed: %s\n", strerror(errno));
            close(client_fd);
            continue;
        }

        struct relay_connection *connection = malloc(sizeof(*connection));
        if (connection == NULL) {
            close(client_fd);
            close(upstream_fd);
            continue;
        }
        connection->client_fd = client_fd;
        connection->upstream_fd = upstream_fd;

        pthread_t thread;
        int thread_result = pthread_create(&thread, NULL, relay_connection_main, connection);
        if (thread_result != 0) {
            fprintf(stderr, "pthread_create failed: %s\n", strerror(thread_result));
            free(connection);
            close(client_fd);
            close(upstream_fd);
            continue;
        }
        pthread_detach(thread);
    }

    if (listener_fd >= 0) {
        close(listener_fd);
        listener_fd = -1;
    }
    if (unix_socket_path != NULL) {
        unlink(unix_socket_path);
    }
    return 0;
}

int main(int argc, char **argv) {
    if (argc != 5) {
        fprintf(stderr, "usage: %s <tcp|unix> <listen-endpoint> <target-ipv4> <target-port>\n", argv[0]);
        return 2;
    }

    uint16_t target_port;
    if (!parse_port(argv[4], &target_port)) {
        fprintf(stderr, "invalid target port: %s\n", argv[4]);
        return 2;
    }
    struct in_addr target_address;
    if (inet_pton(AF_INET, argv[3], &target_address) != 1) {
        fprintf(stderr, "invalid target IPv4 address: %s\n", argv[3]);
        return 2;
    }

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, stop_listener);
    signal(SIGTERM, stop_listener);

    int listening_fd;
    if (strcmp(argv[1], "tcp") == 0) {
        uint16_t listen_port;
        if (!parse_port(argv[2], &listen_port)) {
            fprintf(stderr, "invalid listen port: %s\n", argv[2]);
            return 2;
        }
        listening_fd = create_tcp_listener(listen_port);
    } else if (strcmp(argv[1], "unix") == 0) {
        unix_socket_path = argv[2];
        listening_fd = create_unix_listener(argv[2]);
    } else {
        fprintf(stderr, "invalid mode: %s\n", argv[1]);
        return 2;
    }

    if (listening_fd < 0) {
        fprintf(stderr, "listener creation failed: %s\n", strerror(errno));
        return 1;
    }
    return run_relay(listening_fd, argv[1], argv[2], argv[3], target_port);
}
