#include <netdb.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/utsname.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "debug.h"
#include "config.h"

struct ConfigInfo config_info;

/* remove space, tab and line return from the line */
void clean_up_line (char *line)
{
    char *i = line;
    char *j = line;

    while (*j != 0) {
        *i = *j;
        j += 1;
        if (*i != ' ' && *i != '\t' && *i != '\r' && *i != '\n') {
            i += 1;
        }
    }
    *i = 0;
}

int is_ip_local(const char *ip_to_check) {
    int ret = 0;
    struct ifaddrs *ifaddr;
    char host[NI_MAXHOST];

    ret = getifaddrs(&ifaddr);
    check (ret == 0, "Failed to get network interface on the host machine.");

    for (struct ifaddrs *ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL)
            continue;

        int family = ifa->ifa_addr->sa_family;
        if (family == AF_INET) {
            ret = getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in), 
                host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
            check (ret == 0, "Failed to get name info of network interface.");
            if (strcmp(ip_to_check, host) == 0) {
                freeifaddrs(ifaddr);
                return 1;
            }
        }
    }
    freeifaddrs(ifaddr);
    return 0;
error:
    freeifaddrs(ifaddr);
    return -1;
}

int get_rank ()
{
    int			ret	    = 0;
    uint32_t		i	    = 0;
    uint32_t		num_servers = config_info.num_servers;
    uint32_t		num_clients = config_info.num_clients;
    struct utsname	utsname_buf;
    char		hostname[64];

    /* get hostname */
    ret = uname (&utsname_buf);
    check (ret == 0, "Failed to call uname");

    strncpy (hostname, utsname_buf.nodename, sizeof(hostname));

    config_info.rank = -1;
    for (i = 0; i < num_servers; i++) {
        if (is_ip_local(config_info.servers[i])) {
            config_info.rank      = i;
            config_info.is_server = true;
            break;
        }
    }

    for (i = 0; i < num_clients; i++) {
        if (is_ip_local(config_info.clients[i])) {
            if (config_info.rank == -1) {
                config_info.rank      = i;
                config_info.is_server = false;
                break;
            } else {
                printf("node (%s) listed as both server and client\n", hostname);
            }
        }
    }
    check (config_info.rank >= 0, "Failed to get rank for node: %s", hostname);
    return 0;
 error:
    return -1;
}

int parse_node_list (char *line, char ***node_list)
{
    int num_nodes = 1, k = 0, begin = 0, end = 0;
    char *i = line;

    while (*i != 0) {
        if (*i == '|') ++num_nodes;
        i += 1;
    }
    check (num_nodes >= 1, "Invaild number of nodes: %d", num_nodes);

    *node_list = (char **) calloc (num_nodes, sizeof(char *));
    if (*node_list == NULL){
        printf ("Failed to allocate node_list.\n");
        return 0;
    }
    
    for (k = 0; k < num_nodes; k++) {
        (*node_list)[k] = (char *) calloc (128, sizeof(char));
        check ((*node_list)[k] != NULL, "Failed to allocate node_list[%d]", k);

        while ((*(line + end) != 0) && (*(line + end) != '|')) ++end;
        strncpy((*node_list)[k], line + begin, end - begin);
        begin = end + 1;
        end = begin;
    }

    return num_nodes;

 error:
    return -1;
}

int parse_config_file (char *fname)
{
    int ret = 0;
    FILE *fp = NULL;
    char line[128] = {'\0'};
    int  attr = 0;

    fp = fopen (fname, "r");
    check (fp != NULL, "Failed to open config file %s", fname);

    while (fgets(line, 128, fp) != NULL) {
        // skip comments
        if (strstr(line, "#") != NULL) {
            continue;
        }

        clean_up_line (line);

	    if (strstr (line, "servers:")) {
            attr = ATTR_SERVERS;
            continue;
        } else if (strstr (line, "clients:")) {
            attr = ATTR_CLIENTS;
            continue;
        } else if (strstr (line, "msg_size:")) {
            attr = ATTR_MSG_SIZE;
            continue;
        } else if (strstr (line, "num_concurr_msgs:")) {
            attr = ATTR_NUM_CONCURR_MSGS;
            continue;
        } else if (strstr (line, "qp_num:")) {
            attr = ATTR_QP_NUM;
            continue;
        } else if (strstr (line, "thread_num:")) {
            attr = ATTR_THREAD_NUM;
            continue;
        }

	    if (attr == ATTR_SERVERS) {
            ret = parse_node_list (line, &config_info.servers);
            check (ret > 0, "Failed to get server list");
            config_info.num_servers = ret;
        } else if (attr == ATTR_CLIENTS) {
            ret = parse_node_list (line, &config_info.clients);
            check (ret > 0, "Failed to get client list");
            config_info.num_clients = ret;
        } else if (attr == ATTR_MSG_SIZE) {
            config_info.msg_size = atoi(line);
            check (config_info.msg_size > 0,
                   "Invalid Value: msg_size = %d", config_info.msg_size);
        } else if (attr == ATTR_NUM_CONCURR_MSGS) {
            config_info.num_concurr_msgs = atoi(line);
            check (config_info.num_concurr_msgs > 0,
                   "Invalid Value: num_concurr_msgs = %d",
                   config_info.num_concurr_msgs);
        } else if (attr == ATTR_QP_NUM) {
            config_info.qp_num = atoi(line);
            check (config_info.qp_num > 0,
                   "Invalid Value: qp_num = %d", config_info.qp_num);
        } else if (attr == ATTR_THREAD_NUM) {
            config_info.thread_num = atoi(line);
            check (config_info.thread_num > 0,
                   "Invalid Value: thread_num = %d", config_info.thread_num);
        }
        attr = 0;
    }
    check (config_info.qp_num % config_info.thread_num == 0,
        "qp_num should be a multiple of thread_num : qp_num = %d, thread_num = %d", 
        config_info.qp_num, config_info.thread_num);

    ret = get_rank ();
    check (ret == 0, "Failed to get rank");

    fclose (fp);

    return 0;

 error:
    if (fp != NULL) {
        fclose (fp);
    }
    return -1;
}

void destroy_config_info ()
{
    int num_servers = config_info.num_servers;
    int num_clients = config_info.num_clients;
    int i;

    if (config_info.servers != NULL) {
        for (i = 0; i < num_servers; i++) {
            if (config_info.servers[i] != NULL) {
                free (config_info.servers[i]);
            }
        }
        free (config_info.servers);
    }

    if (config_info.clients != NULL) {
        for (i = 0; i < num_clients; i++) {
            if (config_info.clients[i] != NULL) {
                free (config_info.clients[i]);
            }
        }
        free (config_info.clients);
    }
}

void print_config_info ()
{
    log (LOG_SUB_HEADER, "Configuraion");

    if (config_info.is_server) {
	log ("is_server                 = %s", "true");
    } else {
	log ("is_server                 = %s", "false");
    }
    log ("rank                      = %d", config_info.rank);
    log ("msg_size                  = %d", config_info.msg_size);
    log ("num_concurr_msgs          = %d", config_info.num_concurr_msgs);
    log ("qp_num                    = %d", config_info.qp_num);
    log ("thread_num                = %d", config_info.thread_num);
    log ("sock_port                 = %s", config_info.sock_port);
    
    log (LOG_SUB_HEADER, "End of Configuraion");
}
