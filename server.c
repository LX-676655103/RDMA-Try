#define _GNU_SOURCE
#include <stdlib.h>
#include <stdbool.h>
#include <sys/time.h>
#include <math.h>

#include "debug.h"
#include "ib.h"
#include "setup_ib.h"
#include "config.h"
#include "server.h"

pthread_mutex_t mutex_arr;
pthread_cond_t cond;
int arrived = 0;
double *speed_array;

void barrier()
{
	pthread_mutex_lock(&mutex_arr);
	arrived++;
	if (arrived < config_info.thread_num) {
		pthread_cond_wait(&cond, &mutex_arr);
	}
	else {
		pthread_cond_broadcast(&cond);
    	arrived=0;
  	}
	pthread_mutex_unlock(&mutex_arr);
}

double gettime(void)
{
    struct timeval tv;
    gettimeofday (&tv, NULL);
    return (double)((int64_t)tv.tv_sec * 1000000 + tv.tv_usec) / 1000000.;
}

void *server_thread (void *arg)
{
    int         ret		 = 0, i = 0, j = 0, n = 0, over = 0;
    long        thread_id	 = (long) arg;
    int         num_concurr_msgs = config_info.num_concurr_msgs;
    int         msg_size	 = config_info.msg_size;
    int         num_peers    = ib_res_array[thread_id].num_qps;

    pthread_t   self;
    cpu_set_t   cpuset;

    int                  num_wc		= 20;
    struct ibv_qp       **qp		= ib_res_array[thread_id].qp;
    struct ibv_cq       *cq		= ib_res_array[thread_id].cq;
    struct ibv_srq      *srq            = ib_res_array[thread_id].srq;
    struct ibv_wc       *wc             = NULL;
    uint32_t             lkey           = ib_res_array[thread_id].mr->lkey;
    
    char                *buf_ptr	= ib_res_array[thread_id].ib_buf;
    char                *buf_base	= ib_res_array[thread_id].ib_buf;
    int                  buf_offset	= 0;
    size_t               buf_size	= ib_res_array[thread_id].ib_buf_size;
    
    uint32_t            imm_data	= 0;
    int			num_acked_peers = 0;
    bool                stop            = false;
    struct timeval      start, end;
    long                ops_count	= 0, ops_result = 0;
    double              duration	= 0.0, t1 = 0.0, t2 = 0.0;
    double              throughput	= 0.0;

    /* set thread affinity */
    CPU_ZERO (&cpuset);
    CPU_SET  ((int)thread_id, &cpuset);
    self = pthread_self ();
    ret  = pthread_setaffinity_np (self, sizeof(cpu_set_t), &cpuset);
    check (ret == 0, "thread[%ld]: failed to set thread affinity", thread_id);

    /* pre-post recvs */
    wc = (struct ibv_wc *) calloc (num_wc, sizeof(struct ibv_wc));
    check (wc != NULL, "thread[%ld]: failed to allocate wc.", thread_id);

    for (i = 0; i < num_peers; i++) {
        for (j = 0; j < num_concurr_msgs; j++) {
            ret = post_srq_recv (msg_size, lkey, (uint64_t)buf_ptr, srq, buf_ptr);
            buf_offset = (buf_offset + msg_size) % buf_size;
            buf_ptr = buf_base + buf_offset;
        }
    }
    /* signal the client to start */
    barrier();
    // printf("thread[%ld]: QP number = %d.\n", thread_id, num_peers);
    for (i = 0; i < num_peers; i++) {
        ret = post_send (0, lkey, 0, MSG_CTL_START, qp[i], buf_base);
        check (ret == 0, "thread[%ld]: failed to signal the client to start", thread_id);
    }

    while (stop != true) {
        /* poll cq */
        n = ibv_poll_cq (cq, num_wc, wc);
        check (n >= 0, "thread[%ld]: Failed to poll cq", thread_id);

        for (i = 0; i < n; i++) 
        {
            if (wc[i].status != IBV_WC_SUCCESS) {
                check (wc[i].opcode == IBV_WC_SEND, "thread[%ld]: recv failed status: %s", 
                    thread_id, ibv_wc_status_str(wc[i].status));
                check (0, "thread[%ld]: send failed status: %s", 
                    thread_id, ibv_wc_status_str(wc[i].status));
            }
	    
	        if (wc[i].opcode == IBV_WC_RECV) {
                ops_count += 1;
                debug ("ops_count = %ld", ops_count);
                if (ops_count == NUM_WARMING_UP_OPS) {
                    gettimeofday (&start, NULL);
                    t1 = gettime();
                }
                // if (ops_count == TOT_NUM_OPS) {
                //     gettimeofday (&end, NULL);
                // }
                if (!over && ops_count > NUM_WARMING_UP_OPS && ops_count % COUNT == 0) {
                    t2 = gettime();
                    if(ops_count == TOT_NUM_OPS || t2 - t1 > 0.5){
                        ops_result = ops_count - NUM_WARMING_UP_OPS;
                        gettimeofday (&end, NULL);
                        over = 1;
                    }
                }
                if (ops_count == TOT_NUM_OPS + EXTRA_NUM_OPS) {
                    stop = true;
                    break;
                }
                /* echo the message back */
		        imm_data = ntohl(wc[i].imm_data);
                char *msg_ptr = (char *)wc[i].wr_id;
                post_send (msg_size, lkey, 0, imm_data, qp[imm_data], msg_ptr);
                /* post a new receive */
                post_srq_recv (msg_size, lkey, wc[i].wr_id, srq, msg_ptr);
            }
        }
    }

    /* signal the client to stop */
    for (i = 0; i < num_peers; i++) {
        ret = post_send (0, lkey, IB_WR_ID_STOP, MSG_CTL_STOP, qp[i], ib_res_array[thread_id].ib_buf);
        check (ret == 0, "thread[%ld]: failed to signal the client to stop", thread_id);
    }

    stop = false;
    while (stop != true) {
        /* poll cq */
        n = ibv_poll_cq (cq, num_wc, wc);
        check (n >= 0, "thread[%ld]: Failed to poll cq", thread_id);

	    for (i = 0; i < n; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                check (wc[i].opcode == IBV_WC_SEND, "thread[%ld]: recv failed status: %s",
                    thread_id, ibv_wc_status_str(wc[i].status));
                check (0, "thread[%ld]: send failed status: %s",
                    thread_id, ibv_wc_status_str(wc[i].status));
            }
            if (wc[i].opcode == IBV_WC_SEND && wc[i].wr_id == IB_WR_ID_STOP) {
                num_acked_peers += 1;
                if (num_acked_peers == num_peers) {
                    stop = true;
                    break;
                }
            }
        }
    }
    
    /* dump statistics */
    duration   = (double)((end.tv_sec - start.tv_sec) * 1000000 +
                          (end.tv_usec - start.tv_usec));
    throughput = ops_result / duration;
    speed_array[thread_id] = throughput;
    log ("thread[%ld]: throughput = %f (Mops/s), duration = %f (us)",  thread_id, throughput, duration);

    free (wc);
    pthread_exit ((void *)0);

 error:
    if (wc != NULL) {
    	free (wc);
    }
    pthread_exit ((void *)-1);
}

int run_server ()
{
    int   ret         = 0;
    long  num_threads = config_info.thread_num;
    long  i           = 0;

    pthread_t           *threads = NULL;
    pthread_attr_t       attr;
    void                *status;

    pthread_attr_init (&attr);
    pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_JOINABLE);

    threads = (pthread_t *) calloc (num_threads, sizeof(pthread_t));
    check (threads != NULL, "Failed to allocate threads.");

    speed_array = (double *) calloc(num_threads, sizeof(double));
    check (speed_array != NULL, "Failed to allocate speed_array.");

    for (i = 0; i < num_threads; i++) {
        ret = pthread_create (&threads[i], &attr, server_thread, (void *)i);
        check (ret == 0, "Failed to create server_thread[%ld]", i);
    }

    bool thread_ret_normally = true;
    for (i = 0; i < num_threads; i++) {
        ret = pthread_join (threads[i], &status);
        check (ret == 0, "Failed to join thread[%ld].", i);
        if ((long)status != 0) {
            thread_ret_normally = false;
            log ("server_thread[%ld]: failed to execute", i);
        }
    }

    if (thread_ret_normally == false) {
        goto error;
    }

    pthread_attr_destroy    (&attr);
    free (threads);

    double sum_speed = 0.0, sqrt_sum_speed = 0.0, variance = 0.0, mean_speed = 0.0;
    for (i = 0; i < num_threads; i++) {
        sum_speed += speed_array[i];
        sqrt_sum_speed += (speed_array[i] * speed_array[i]);
    }
    mean_speed = sum_speed / num_threads;
    variance = sqrt((num_threads * sqrt_sum_speed - sum_speed * sum_speed) 
        / (num_threads * (num_threads - 1))) / mean_speed * 100;
    
    log ("(%.1f%%)total throughput = %f (Mops/s)",  variance, sum_speed);

    return 0;

 error:
    if (threads != NULL) {
        free (threads);
    }
    pthread_attr_destroy    (&attr);
    
    return -1;
}
