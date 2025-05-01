#include <stdio.h>
#include <string.h>
#include <voltlane/clientcom.h>

// The following example opens a new connection to the connserver
// and subsequently enters a REPL mode; you type a message, and it's
// sent via the voltlane protocol to the master server, which is connected
// to the connserver. If a connection error occurs, a reconnection is
// attempted using the voltlane protocol's asymmetric key authentication.

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
            fprintf(stderr, "lost connection: %s\n", vl_get_last_error());
            if (vl_connection_reconnect(conn) < 0) {
                exit_code = 1;
                break;
            } else {
                fprintf(stderr, "reconnected!\n");
                continue;
            }
        }

        vl_message msg = vl_connection_recv(conn);
        if (!msg.data) {
            fprintf(stderr, "lost connection: %s\n", vl_get_last_error());
            if (vl_connection_reconnect(conn) < 0) {
                exit_code = 1;
                break;
            } else {
                fprintf(stderr, "reconnected!\n");
                continue;
            }
        }
        printf("%.*s", (int)msg.size, msg.data);
    }

cleanup:
    vl_connection_free(conn);
    return exit_code;
}
