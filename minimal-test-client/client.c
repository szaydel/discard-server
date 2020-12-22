#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/socket.h>
#include <arpa/inet.h>

#define HOSTNAME "127.0.0.1"
#define PORT     5002

typedef struct options options;

struct options {
    uint16_t port;
    char hostname[256];
    char* msg;
};

int main(int argc, char *argv[]) {
    int sock;
    struct sockaddr_in server;
    options config = {0};

    int opt;
    while ((opt = getopt(argc, argv, "h:p:m:")) != -1) {
        switch (opt) {
        case 'h':
            strncpy(config.hostname, optarg, sizeof config.hostname);
            break;
        case 'm':
            if (optarg) {
                config.msg = optarg;
            }
            break;
        case 'p':
            config.port = (uint16_t)atoi(optarg);
            break;
        default: /* '?' */
            fprintf(stderr, "Usage: %s [-h <host>] [-p <port>]\n",
                    argv[0]);
            exit(EXIT_FAILURE);
        }
    }

    if (!strlen(config.hostname)) {
        strcpy(config.hostname, HOSTNAME);
    }

    if (!config.port) {
        config.port = PORT;
    }

    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        perror("socket(...)");
        return EXIT_FAILURE;
    }
    server.sin_addr.s_addr = inet_addr(config.hostname);
    server.sin_family = AF_INET;
    server.sin_port = htons(config.port);

    if (connect(sock , (struct sockaddr *)&server , sizeof(server)) < 0) {
        perror("connect(...)");
        return EXIT_FAILURE;
    }

    while (1) {
        if(send(sock, config.msg, strlen(config.msg) , 0) < 0) {
            perror("send(...)");
            return EXIT_FAILURE;
        }
    }

    return EXIT_SUCCESS;
}
