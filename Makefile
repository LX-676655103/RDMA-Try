CC=gcc
CFLAGS=-Wall -O2
INCLUDES=
LDFLAGS=
LIBS=-pthread -lrdmacm -libverbs -lm

SRCS=main.c client.c config.c ib.c server.c setup_ib.c sock.c
OBJS=$(SRCS:.c=.o)
PROG=rdma-tutorial

all: $(PROG)

debug: CFLAGS=-Wall -g -DDEBUG -DLIBIBVERBS_DEBUG=1
debug: $(PROG)

.c.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c -o $@ $<

$(PROG): $(OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $(OBJS) $(LDFLAGS) $(LIBS)

clean:
	$(RM) *.o *~ $(PROG)
