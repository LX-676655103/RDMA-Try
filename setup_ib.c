#include <arpa/inet.h>
#include <unistd.h>
#include <malloc.h>

#include "sock.h"
#include "ib.h"
#include "debug.h"
#include "config.h"
#include "setup_ib.h"

struct IBRes *ib_res_array;

int connect_qp_server ()
{
    int			 ret		= 0, n = 0, i = 0, j = 0, k = 0;
    int          num_peers  = config_info.num_clients;
    int          num_threads= config_info.thread_num;
    int          num_qps    = ib_res_array[0].num_qps;
    int          num_qps_peer = num_qps / num_peers;
    int			 sockfd		= 0;
    int			*peer_sockfd	= NULL;
    struct sockaddr_in	 peer_addr;
    socklen_t		 peer_addr_len	= sizeof(struct sockaddr_in);
    char sock_buf[64]			= {'\0'};
    struct QPInfo	*local_qp_info	= NULL;
    struct QPInfo	*remote_qp_info = NULL;

    printf("Number of peer node: %d, thread: %d, %d qps/thread\n", 
        num_peers, num_threads, num_qps);

    /* create socket and bind to port */
    sockfd = sock_create_bind(config_info.sock_port);
    check(sockfd > 0, "Failed to create server socket.");
    listen(sockfd, 5);

    peer_sockfd = (int *) calloc (num_peers, sizeof(int));
    check (peer_sockfd != NULL, "Failed to allocate peer_sockfd");

    for (i = 0; i < num_peers; i++) {
        peer_sockfd[i] = accept(sockfd, (struct sockaddr *)&peer_addr, &peer_addr_len);
        check (peer_sockfd[i] > 0, "Failed to create peer_sockfd[%d]", i);
    }

    log (LOG_SUB_HEADER, "Start of IB Config");
    for (i = 0; i < num_threads; i++) {
        /* get RoCE gid */
        union ibv_gid local_gid;
        ret = ibv_query_gid(ib_res_array[i].ctx, IB_PORT, 0, &local_gid);
        check (ret == 0, "Failed to query gidfor port %d, index %d\n", IB_PORT, 0);

        /* init local qp_info */
        local_qp_info = (struct QPInfo *) calloc (num_qps, sizeof(struct QPInfo));
        check (local_qp_info != NULL, "Failed to allocate local_qp_info");
        for (j = 0; j < num_qps; j++) {
            local_qp_info[j].lid	= ib_res_array[i].port_attr.lid; 
            local_qp_info[j].qp_num = ib_res_array[i].qp[j]->qp_num;
            local_qp_info[j].rank   = config_info.rank;
            local_qp_info[j].addr   = (uintptr_t)ib_res_array[i].ib_buf;
            local_qp_info[j].rkey   = ib_res_array[i].mr->rkey;
            memcpy(local_qp_info[j].gid , &local_gid, 16);
        }
        /* get qp_info from client */
        remote_qp_info = (struct QPInfo *) calloc (num_qps, sizeof(struct QPInfo));
        check (remote_qp_info != NULL, "Failed to allocate remote_qp_info");

        for (j = 0; j < num_qps_peer; j++) {
            for (k = 0; k < num_peers; k++) {
                ret = sock_get_qp_info (peer_sockfd[k],
                    &remote_qp_info[k * num_qps_peer + j]);
                check (ret == 0, "Failed to get qp_info from client[%d] to remote_qp_info[%d]", 
                    k, k * num_qps_peer + j);
            }
        }
        /* send qp_info to client */
        for (j = 0; j < num_qps_peer; j++) {
            for (k = 0; k < num_peers; k++) {
                ret = sock_set_qp_info (peer_sockfd[k],
                    &local_qp_info[k * num_qps_peer + j]);
                check (ret == 0, "Failed to send local_qp_info[%d] to client[%d]", 
                    k * num_qps_peer + j, k);
            }
        }
        /* change send QP state to RTS */
        for (j = 0; j < num_qps_peer; j++) {
            for (k = 0; k < num_peers; k++) {
                ret = modify_qp_to_rts (
                    ib_res_array[i].qp[k * num_qps_peer + j], 
                    remote_qp_info[k * num_qps_peer + j].qp_num, 
                    remote_qp_info[k * num_qps_peer + j].lid,
                    remote_qp_info[k * num_qps_peer + j].gid);
                check (ret == 0, "Failed to modify qp[%d] to rts", k * num_qps_peer + j);

                // log ("\tqp[%"PRIu32"] <-> qp[%"PRIu32"]", 
                //     ib_res_array[i].qp[k * num_qps_peer + j]->qp_num, 
                //     remote_qp_info[k * num_qps_peer + j].qp_num);
            }
        }
        free (local_qp_info);
        free (remote_qp_info);
    }
    log (LOG_SUB_HEADER, "End of IB Config");

    /* sync with clients */
    for (i = 0; i < num_peers; i++) {
        n = sock_read (peer_sockfd[i], sock_buf, sizeof(SOCK_SYNC_MSG));
        check (n == sizeof(SOCK_SYNC_MSG), "Failed to receive sync from client");
    }
    
    for (i = 0; i < num_peers; i++) {
        n = sock_write (peer_sockfd[i], sock_buf, sizeof(SOCK_SYNC_MSG));
        check (n == sizeof(SOCK_SYNC_MSG), "Failed to write sync to client");
    }
	
    for (i = 0; i < num_peers; i++) {
        close (peer_sockfd[i]);
    }
    free (peer_sockfd);
    close (sockfd);
    return 0;
 error:
    if (peer_sockfd != NULL) {
        for (i = 0; i < num_peers; i++) {
            if (peer_sockfd[i] > 0) 
                close (peer_sockfd[i]);
        }
        free (peer_sockfd);
    }
    if (sockfd > 0) close (sockfd);
    if (local_qp_info != NULL)
	    free (local_qp_info);
    if (remote_qp_info != NULL)
	    free (remote_qp_info);
    return -1;
}

int connect_qp_client ()
{
    int ret	       = 0, n = 0, i = 0, j = 0, k = 0;
    int num_peers  = config_info.num_servers;
    int          num_threads= config_info.thread_num;
    int          num_qps    = ib_res_array[0].num_qps;
    int          num_qps_peer = num_qps / num_peers;
    int *peer_sockfd   = NULL;
    char sock_buf[64]  = {'\0'};

    struct QPInfo *local_qp_info  = NULL;
    struct QPInfo *remote_qp_info = NULL;

    printf("Number of peer node: %d, thread: %d, %d qps/thread\n", 
        num_peers, num_threads, num_qps);

    peer_sockfd = (int *) calloc (num_peers, sizeof(int));
    check (peer_sockfd != NULL, "Failed to allocate peer_sockfd");

    for (i = 0; i < num_peers; i++) {
        peer_sockfd[i] = sock_create_connect (
            config_info.servers[i], config_info.sock_port);
        check (peer_sockfd[i] > 0, "Failed to create peer_sockfd[%d]", i);
    }

    log (LOG_SUB_HEADER, "Start of IB Config");
    for (i = 0; i < num_threads; i++) {
        /* get RoCE gid */
        union ibv_gid local_gid;
        ret = ibv_query_gid(ib_res_array[i].ctx, IB_PORT, 0, &local_gid);
        check (ret == 0, "Failed to query gidfor port %d, index %d\n", IB_PORT, 0);

        /* init local qp_info */
        local_qp_info = (struct QPInfo *) calloc (num_qps, sizeof(struct QPInfo));
        check (local_qp_info != NULL, "Failed to allocate local_qp_info");

        for (j = 0; j < num_qps; j++) {
            local_qp_info[j].lid	= ib_res_array[i].port_attr.lid; 
            local_qp_info[j].qp_num = ib_res_array[i].qp[j]->qp_num;
            local_qp_info[j].rank   = config_info.rank;
            local_qp_info[j].addr   = (uintptr_t)ib_res_array[i].ib_buf;
            local_qp_info[j].rkey   = ib_res_array[i].mr->rkey;
            memcpy(local_qp_info[j].gid , &local_gid, 16);
        }
        /* send qp_info to server */
        for (j = 0; j < num_qps_peer; j++) {
            for (k = 0; k < num_peers; k++) {
                ret = sock_set_qp_info (peer_sockfd[k],
                    &local_qp_info[k * num_qps_peer + j]);
                check (ret == 0, "Failed to send local_qp_info[%d] to client[%d]", 
                    k * num_qps_peer + j, k);
            }
        }
        /* get qp_info from server */    
        remote_qp_info = (struct QPInfo *) calloc (num_qps, sizeof(struct QPInfo));
        check (remote_qp_info != NULL, "Failed to allocate remote_qp_info");

        for (j = 0; j < num_qps_peer; j++) {
            for (k = 0; k < num_peers; k++) {
                ret = sock_get_qp_info (peer_sockfd[k],
                    &remote_qp_info[k * num_qps_peer + j]);
                check (ret == 0, "Failed to get qp_info from client[%d] to remote_qp_info[%d]", 
                    k, k * num_qps_peer + j);
            }
        }
        log ("get qp_info from server.");
        /* change send QP state to RTS */
        for (j = 0; j < num_qps_peer; j++) {
            for (k = 0; k < num_peers; k++) {
                ret = modify_qp_to_rts (
                    ib_res_array[i].qp[k * num_qps_peer + j], 
                    remote_qp_info[k * num_qps_peer + j].qp_num, 
                    remote_qp_info[k * num_qps_peer + j].lid,
                    remote_qp_info[k * num_qps_peer + j].gid);
                check (ret == 0, "Failed to modify qp[%d] to rts", k * num_qps_peer + j);

                log ("\tqp[%"PRIu32"] <-> qp[%"PRIu32"]", 
                    ib_res_array[i].qp[k * num_qps_peer + j]->qp_num, 
                    remote_qp_info[k * num_qps_peer + j].qp_num);
            }
        }
        free (local_qp_info);
        free (remote_qp_info);
    }
    log (LOG_SUB_HEADER, "End of IB Config");

    /* sync with server */
    for (i = 0; i < num_peers; i++) {
        n = sock_write (peer_sockfd[i], sock_buf, sizeof(SOCK_SYNC_MSG));
        check (n == sizeof(SOCK_SYNC_MSG), "Failed to write sync to client[%d]", i);
    }
    
    for (i = 0; i < num_peers; i++) {
        n = sock_read (peer_sockfd[i], sock_buf, sizeof(SOCK_SYNC_MSG));
        check (n == sizeof(SOCK_SYNC_MSG), "Failed to receive sync from client");
    }

    for (i = 0; i < num_peers; i++) {
	    close (peer_sockfd[i]);
    }
    free (peer_sockfd);
    return 0;

 error:
    if (peer_sockfd != NULL) {
        for (i = 0; i < num_peers; i++) {
            if (peer_sockfd[i] > 0)
                close (peer_sockfd[i]);
        }
        free (peer_sockfd);
    }
    if (local_qp_info != NULL)
	    free (local_qp_info);
    if (remote_qp_info != NULL)
	    free (remote_qp_info);
    return -1;
}

int setup_ib ()
{
    int	ret	= 0, i = 0, j = 0;
    struct ibv_device **dev_list = NULL;

    log (LOG_SUB_HEADER, "Start of IB Setup");
    /* init ib res for every thread */
    ib_res_array = (struct IBRes*) calloc(
        config_info.thread_num, sizeof(struct IBRes));
    check (ib_res_array != NULL, "Failed to allocate ib_res_array.");
    memset (ib_res_array, 0, sizeof(struct IBRes) * config_info.thread_num);

    /* get IB device list */
    dev_list = ibv_get_device_list(NULL);
    check(dev_list != NULL, "Failed to get ib device list.");

    for (i = 0; i < config_info.thread_num; ++i) {
        /* set qp num */
        if (config_info.is_server) {
            ib_res_array[i].num_qps = config_info.num_clients * 
                config_info.qp_num / config_info.thread_num;
        } else {
            ib_res_array[i].num_qps = config_info.num_servers * 
                config_info.qp_num / config_info.thread_num;
        }
        /* create IB context and open the device */
        ib_res_array[i].ctx = ibv_open_device(*dev_list);
        check(ib_res_array[i].ctx != NULL, "Failed to open ib device.");

        /* allocate protection domain */
        ib_res_array[i].pd = ibv_alloc_pd(ib_res_array[i].ctx);
        check(ib_res_array[i].pd != NULL, "Failed to allocate protection domain.");

        /* query IB port attribute */
        ret = ibv_query_port(ib_res_array[i].ctx, 
            IB_PORT, &ib_res_array[i].port_attr);
        check(ret == 0, "Failed to query IB port information.");

        /* register mr */
        ib_res_array[i].ib_buf_size = config_info.msg_size * 
            config_info.num_concurr_msgs * ib_res_array[i].num_qps;
        ib_res_array[i].ib_buf = (char *) memalign (4096, ib_res_array[i].ib_buf_size);
        check (ib_res_array[i].ib_buf != NULL, "Failed to allocate ib_buf");

        ib_res_array[i].mr = ibv_reg_mr (ib_res_array[i].pd, 
            (void *)ib_res_array[i].ib_buf, ib_res_array[i].ib_buf_size,
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
        check (ib_res_array[i].mr != NULL, "Failed to register mr");
        
        /* query IB device attr */
        ret = ibv_query_device(ib_res_array[i].ctx, &ib_res_array[i].dev_attr);
        check(ret == 0, "Failed to query device");
        
        /* create cq */
        ib_res_array[i].cq = ibv_create_cq (ib_res_array[i].ctx, 
            ib_res_array[i].dev_attr.max_cqe, NULL, NULL, 0);
        check (ib_res_array[i].cq != NULL, "Failed to create cq");

        /* create srq */
        struct ibv_srq_init_attr srq_init_attr = {
            .attr.max_wr  = ib_res_array[i].dev_attr.max_srq_wr,
            .attr.max_sge = 1,
        };

        ib_res_array[i].srq = ibv_create_srq (ib_res_array[i].pd, &srq_init_attr);
        check (ib_res_array[i].srq != NULL, "Failed to create srq");

        /* create qp */
        struct ibv_qp_init_attr qp_init_attr = {
            .send_cq = ib_res_array[i].cq,
            .recv_cq = ib_res_array[i].cq,
            .srq     = ib_res_array[i].srq,
            .cap = {
                .max_send_wr = MAX_QP_WR,
                .max_recv_wr = MAX_QP_WR,
                .max_send_sge = 1,
                .max_recv_sge = 1,
            },
            .qp_type = IBV_QPT_RC,
        };

        ib_res_array[i].qp = (struct ibv_qp **) calloc (ib_res_array[i].num_qps, sizeof(struct ibv_qp *));
        check (ib_res_array[i].qp != NULL, "Failed to allocate qp");

        for (j = 0; j < ib_res_array[i].num_qps; j++) {
            ib_res_array[i].qp[j] = ibv_create_qp (ib_res_array[i].pd, &qp_init_attr);
            check (ib_res_array[i].qp[j] != NULL, "Failed to create qp[%d]", j);
        }
    }
    log (LOG_SUB_HEADER, "End of IB Setup");
    /* connect QP */
    if (config_info.is_server) {
        ret = connect_qp_server ();
    } else {
        ret = connect_qp_client ();
    }
    check (ret == 0, "Failed to connect qp");

    ibv_free_device_list (dev_list);
    return 0;
 error:
    if (dev_list != NULL) {
        ibv_free_device_list (dev_list);
    }
    return -1;
}

void close_ib_connection ()
{
    int i, j;
    for (i = 0; i < config_info.thread_num; ++i) 
    {
        if (ib_res_array[i].qp != NULL) {
            for (j = 0; j < ib_res_array[i].num_qps; j++) {
                if (ib_res_array[i].qp[j] != NULL)
                    ibv_destroy_qp (ib_res_array[i].qp[j]);
            }
            free (ib_res_array[i].qp);
        }

        if (ib_res_array[i].srq != NULL)
            ibv_destroy_srq (ib_res_array[i].srq);

        if (ib_res_array[i].cq != NULL)
            ibv_destroy_cq (ib_res_array[i].cq);

        if (ib_res_array[i].mr != NULL)
            ibv_dereg_mr (ib_res_array[i].mr);

        if (ib_res_array[i].pd != NULL)
            ibv_dealloc_pd (ib_res_array[i].pd);

        if (ib_res_array[i].ctx != NULL)
            ibv_close_device (ib_res_array[i].ctx);

        if (ib_res_array[i].ib_buf != NULL)
            free (ib_res_array[i].ib_buf);
    }
}
