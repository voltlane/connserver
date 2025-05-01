#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <voltlane/clientcom.h>

int main(void) {
    vl_connection* conn = vl_connection_new("127.0.0.1:42000");
    int exit_code = 0;
    if (!conn) {
        fprintf(stderr, "%s\n", vl_get_last_error());
        exit_code = 1;
        goto cleanup;
    }

    char buf[1024];
    memset(buf, 0, sizeof(buf));

    while (1) {
        buf[sizeof(buf) - 1] = 0;
        char* res = fgets(buf, sizeof(buf) - 1, stdin);
        if (!res) {
            break;
        }
        int rc = vl_connection_send(conn, res, strlen(res));
        if (rc < 0) {
            fprintf(stderr, "%s\n", vl_get_last_error());
            exit_code = 1;
            break;
        }

        vl_message msg = vl_connection_recv(conn);
        if (!msg.data) {
            fprintf(stderr, "%s\n", vl_get_last_error());
            exit_code = 1;
            break;
        }
        printf("%.*s", (int)msg.size, msg.data);
    }

cleanup:
    vl_connection_free(conn);
    return exit_code;
}
