#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/times.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <pthread.h>
#include <time.h>

#define PORT 5555
#define PORT_DB 7777
#define MAXMSG 512
#define maxConn 20
#define hostNameLength 50

/*** Declaration of global variables and structures ***/
char serverConn[maxConn][hostNameLength];			/* Keeps track of other middlewares' IP addresses */
char dbServer[hostNameLength];
int conn_count;				/* conn_count - how many other middlewares are there */
fd_set processingFdSet;
struct thread_data {
	int  thread_id;
	int  socketfd;
	char buffer[MAXMSG];
	
};
/*** End of declaration ***/


/* makeSocket
* Creates and names a socket in the internet
* namespace. The socket created exists
* on the machine from which the function is
* called. Instead of finding and using the
* machine's internet address, the function
* specifies INADDR_ANY as the host address;
* the system replaces that with the machine's
* actual address.
*/
int makeSocket(unsigned short int port) {
	int sock;
	struct sockaddr_in name;

	/* Create a socket. */
	sock = socket(PF_INET, SOCK_STREAM, 0);
	if (sock < 0) {
		perror("Could not create a socket\n");
		exit(EXIT_FAILURE);
	}
	/* Give the socket a name. */
	/* Socket address format set to AF_INET for internet use. */
	name.sin_family = AF_INET;
	/* Set port number. The function htons converts from host byte order to network byte order.*/
	name.sin_port = htons(port);
	/* Set the internet address of the host the function is called from. */
	/* The function htonl converts INADDR_ANY from host byte order to network byte order. */
	/* (htonl does the same thing as htons but the former converts a long integer whereas
	* htons converts a short.)
	*/
	name.sin_addr.s_addr = htonl(INADDR_ANY);
	/* Assign an address to the socket by calling bind. */
	if (bind(sock, (struct sockaddr *)&name, sizeof(name)) < 0) {
		perror("Could not bind a name to the socket\n");
		exit(EXIT_FAILURE);
	}
	return(sock);
}

/* initSocketAddress
* Initialises a sockaddr_in struct given a host name and a port. */
void initSocketAddress(struct sockaddr_in *name, char *hostName, unsigned short int port) {
	struct hostent *hostInfo; /* Contains info about the host */

	/* Socket address format set to AF_INET for internet use. */
	name->sin_family = AF_INET;

	/* Set port number. The function htons converts from host byte order to network byte order.*/
	name->sin_port = htons(port);

	/* Get info about host. */
	hostInfo = gethostbyname(hostName);
	if (hostInfo == NULL) {
		fprintf(stderr, "initSocketAddress - Unknown host %s\n",hostName);
		exit(EXIT_FAILURE);
	}
	/* Fill in the host name into the sockaddr_in struct. */
	name->sin_addr = *(struct in_addr *)hostInfo->h_addr;
}

/* Read data from FD <int fileDescriptor> and put it into <char *buf> */
int readMessage(int fileDescriptor, char *buf) {
	int nOfBytes;
	nOfBytes = read(fileDescriptor, buf, MAXMSG);
	if (nOfBytes < 0) {
		perror("Could not read data from client\n");
		return (-2);
	}
	else if (nOfBytes == 0)
	/* End of file */
	return(-1);
	else
	/* Data read */
	return(0);
}

/* Write the message <char *message> onto FD <int fileDescriptor> */
void writeMessage(int fileDescriptor, char *message) {
	int nOfBytes;

	nOfBytes = write(fileDescriptor, message, strlen(message) + 1);
	if (nOfBytes < 0) {
		perror("writeMessage - Could not write data\n");
		exit(EXIT_FAILURE);
	}
}

/* Checks if the string b is present in the array of strings a
if yes, returns 1, else 0 */
int checkArray(char a[][hostNameLength], int num, char *b) {
	int i;
	for(i=0;i<num;i++) {
		if(!(strcmp(a[i],b)))
		return 1;
	}
	return 0;
}

int dbserverConnectAndTransferTransaction(char *transaction) {
	char hostName[hostNameLength];
	int dbsock;
	struct sockaddr_in serverName;
	
	strncpy(hostName, dbServer, hostNameLength);
	hostName[hostNameLength - 1] = '\0';
	dbsock = socket(PF_INET, SOCK_STREAM, 0);
	if( dbsock < 0 ) {
		perror("Could not create a socket\n");
		exit(EXIT_FAILURE);
	}
	initSocketAddress(&serverName, hostName, PORT_DB);
	if(connect(dbsock, (struct sockaddr *)&serverName, sizeof(serverName)) < 0) {
		perror("Could not connect to database server\n");
		exit(EXIT_FAILURE);
	}
	writeMessage(dbsock, transaction);
	
	return dbsock;
}

/* Thread handle for incoming communication from another middleware */
void * handle_middleware(void * args) {
	int flag, i, j;
	char controlMsgs[MAXMSG];
	int dbsock;
	struct thread_data t, *temp;
	fd_set tempFdSet, readFdSet;

	temp = (struct thread_data *) args;
	t.thread_id = temp->thread_id;
	t.socketfd = temp->socketfd;
	strncpy(t.buffer, temp->buffer, MAXMSG);

	/* Connect to database server and transmit transaction */
	dbsock = dbserverConnectAndTransferTransaction(t.buffer);
	/* End of transaction transmit to database server */

	/* Wait and receive answer from database server and send answer to coordinator */
	FD_ZERO(&tempFdSet);
	FD_SET(dbsock, &tempFdSet);
	readFdSet = tempFdSet;
	printf("Checkpoint - waiting for answer from database server (middleware)!\n");
	while(select(FD_SETSIZE, &readFdSet, NULL, NULL, NULL) <= 0) {
		perror("Select failed\n");
		readFdSet = tempFdSet;
		continue;
	}
	if (FD_ISSET(dbsock, &readFdSet)) {
		j = readMessage(dbsock, controlMsgs);
		if ((j < 0) || (controlMsgs[0] == '0')) {	//Abort
			printf("Received abort from dbserv, sending abort to coordinator! (middleware)\n");
			writeMessage(t.socketfd, "0");
		}
		else if (controlMsgs[0] == '1') {
			printf("Locks acquired! (middleware)!\n");
			writeMessage(t.socketfd, "1");
		}
	}
	else {	//Select unblocked due to unknown reasons
		printf("Select unblocked due to unknown reasons (middleware).\n");
		writeMessage(dbsock, "0");
		writeMessage(t.socketfd, "0");
	}
	/* End of answer receive from database server and sending answer to coordinator */

	/* Receiving answer on what to do from coordinator and forwarding to db server */
	FD_ZERO(&tempFdSet);
	FD_SET(t.socketfd, &tempFdSet);
	readFdSet = tempFdSet;
	while (select(FD_SETSIZE, &readFdSet, NULL, NULL, NULL) <= 0) {
		perror("Select failed\n");
		readFdSet = tempFdSet;
		continue;
	}
	if (FD_ISSET(t.socketfd, &readFdSet)) {
		j = readMessage(t.socketfd, controlMsgs);
		if( (j < 0) || (controlMsgs[0] == '0') ) {
			printf("Received abort from coordinator - aborting!\n");
			writeMessage(dbsock, "0");
			sleep(1);
		}
		else if (controlMsgs[0] == '1') {
			printf("Received COMMIT from coordinator - transmitting to database server (middleware)\n");
			writeMessage(dbsock, "1");
			sleep(1);
		}
	}
	else {	//Select unblocked due to unknown reasons	
		printf("Select unblocked due to unknown reasons (middleware).\n");
		writeMessage(dbsock, "0");
	}
	/* End of answer receive and forward */

	close(dbsock);
	close(t.socketfd);
	pthread_exit(NULL);
}

/* Thread handle for incoming communication from a client */
void * handle_client(void * args) {
	int flag, i, j, k, dbabort;
	char hostName[hostNameLength], controlMsgs[MAXMSG];
	int serversock[maxConn], dbsock;	/* File descriptors for socket connections to other middlewares */
	struct sockaddr_in serverName;
	struct thread_data t, *temp;
	struct timeval tv;
	fd_set serverFdSet, readFdSet, tempFdSet;

	temp = (struct thread_data *) args;
	t.thread_id = temp->thread_id;
	t.socketfd = temp->socketfd;
	strncpy(t.buffer, temp->buffer, MAXMSG);
	srand(time(NULL));
	tv.tv_sec = ( (rand()%101)+50 );
	tv.tv_usec = 0;

    beginning:
	i = 0;
	FD_ZERO(&serverFdSet);
	/* Initiating the connection to other middlewares and transmitting the transaction */
	while (i<conn_count) {
		strncpy(hostName, serverConn[i], hostNameLength);
		hostName[hostNameLength - 1] = '\0';
		serversock[i] = socket(PF_INET, SOCK_STREAM, 0);	//Creating a socket for the connection
		if (serversock[i] < 0) {
			perror("Could not create a socket\n");
			exit(EXIT_FAILURE);
		}
		initSocketAddress(&serverName, hostName, PORT);		//The remote socket to connect to
		if (connect(serversock[i], (struct sockaddr *)&serverName, sizeof(serverName)) < 0) {
			perror("Could not connect to server\n");
			exit(EXIT_FAILURE);
		}
		FD_SET(serversock[i], &serverFdSet);
		writeMessage(serversock[i++], t.buffer);
	}
	/* End of transaction transmit and connection initiation */
	
	/* Connect to database server and transmit transaction */
	dbsock = dbserverConnectAndTransferTransaction(t.buffer);
	/* End of transaction transmit to database server */
	
	dbabort=1;
	/* Wait for and receive answer from database server */
	FD_ZERO(&tempFdSet);
	FD_SET(dbsock, &tempFdSet);
	readFdSet = tempFdSet;
	printf("Checkpoint - waiting for answer from database server (client)!\n");
	while (select(FD_SETSIZE, &readFdSet, NULL, NULL, NULL) <= 0) {
		perror("Select failed\n");
		readFdSet = tempFdSet;
		continue;
	}
	if (FD_ISSET(dbsock, &readFdSet)) {
		j = readMessage(dbsock, controlMsgs);
		if ((j < 0) || (controlMsgs[0] == '0')) {	//Abort
            printf("Received abort from dbserv! (client)\n");
			dbabort=0;
		}
		else if (controlMsgs[0] == '1')
			printf("Locks acquired! (client)!\n");
	}
	else {	//Select unblocked due to unknown reasons
		printf("Select unblocked due to unknown reasons (client).\n");
		dbabort=0;
	}
	/* End of answer receive from database server */

	sleep(1);
	flag = 1; i = 0;
	/* Getting the answers from the other middlewares (on the attempt to lock the required mutexes) */
	printf("Waiting for the answers from the other middlewares timeout sec: %d\n", tv.tv_sec);
	while ((i<conn_count) && flag) {
		readFdSet = serverFdSet;
		while (1) {
			i = select(FD_SETSIZE, &readFdSet, NULL, NULL, &tv);
			if(i < 0) {
				perror("Select failed\n");
				readFdSet = tempFdSet;
				continue;
			}
			if(i == 0) {
				printf("Wait timeout!\n");
				flag = 0;
				break;
			}
			else
				break;
		}
		for (k = 0; ((k < conn_count) && flag); k++) {
			if (FD_ISSET(serversock[k], &readFdSet)) {
				i++;
				FD_CLR(serversock[k], &serverFdSet);
				j = readMessage(serversock[k], controlMsgs);
				if ( j < 0 ) {
					perror("Error while trying to read data from middleware socket (inthread)!\n");
					flag = 0;
					break;
				}
				if (controlMsgs[0] == '0') {	//Answer received - abort
					flag = 0;
					break;
				}
			}
		}
	}
	/* End of getting answers from other middlewares */

	i = 0;
	/* If everyone is ready to commit, send message and commit */
	if (flag && dbabort) {
		printf("Ready to commit! Transmitting permission to all middlewares!\n");
		while (i<conn_count)		//Transmitting permission to commit to all other middlewares
			writeMessage(serversock[i++], "1");
		writeMessage(dbsock, "1");
		writeMessage(t.socketfd, "Transaction successful!\n");
	}
	/* End of transaction commit */

	/* If any of the other middlewares have voted abort */
	else {
		if (!dbabort)
			printf("Abort received from database server\n");
		else
        	printf("Received abort from one of the middlewares (or select timeout), retrying!\n");
		while (i<conn_count) {
			writeMessage(serversock[i], "0");
            		close(serversock[i++]);
		}
		writeMessage(dbsock, "0");
		close(dbsock);
		goto beginning;
	}
	FD_CLR(t.socketfd, &processingFdSet);
	pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
	int sock, clientSocket; 		/* Incoming connections (sock) and communication initialization (clientSocket) */
	int i, j;
	char hostName[hostNameLength];		/* Temporary string used to keep IP addresses */
	struct sockaddr_in clientName;		/* Temporary address structs used during connection initialization */
	size_t size;
	fd_set activeFdSet, readFdSet, serverFdSet; 		/* Used by select */

	/* Thread declarations */
	pthread_t thread[maxConn];
	pthread_attr_t attr;
	int thread_counter;				/* Which thread is next to be used */
	struct thread_data t[maxConn];	/* Data structure to pass to thread handler */
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	/* End of thread declarations */

	strcpy(dbServer, "127.0.0.1");
	j = thread_counter = 0;
	/* Create a socket and set it up to accept connections */
	sock = makeSocket(PORT);
	/* Listen for connection requests from clients */
	if (listen(sock,3) < 0) {
		perror("Could not listen for connections\n");
		exit(EXIT_FAILURE);
	}
	/* Initialise the set of active sockets */
	FD_ZERO(&activeFdSet);
	FD_ZERO(&readFdSet);
	FD_ZERO(&processingFdSet);
	FD_ZERO(&serverFdSet);
	FD_SET(sock, &activeFdSet);

	conn_count=argc-1;

	/* Copy other middlewares' IP addresses to a global array */
	while (j<conn_count)
		strncpy(serverConn[j++], argv[j+1], hostNameLength);

	while (1) {
		/* Block until input arrives on one or more active sockets FD_SETSIZE is a constant with value = 1024 */
		readFdSet = activeFdSet;
		if (select(FD_SETSIZE, &readFdSet, NULL, NULL, NULL) <= 0) {
			perror("Select failed\n");
			continue;
		}

		/* Service all the sockets with input pending */
		for (i = 0; i < FD_SETSIZE; ++i) {
			if (FD_ISSET(i, &readFdSet)) {
				/* Incoming connection on original socket */
				if (i == sock) {
					size = sizeof(clientName);
					clientSocket = accept(sock, (struct sockaddr *)&clientName, &size);
					if(clientSocket < 0) {
						perror("Could not accept connection\n");
						exit(EXIT_FAILURE);
					}
					strcpy(hostName,inet_ntoa(clientName.sin_addr));

					/* Middleware initiating connection */
					if((checkArray(serverConn, conn_count, hostName))) {
						printf("Incoming connection from server %s, port %hd\n", inet_ntoa(clientName.sin_addr), ntohs(clientName.sin_port));
						FD_SET(clientSocket, &activeFdSet);
						FD_SET(clientSocket, &serverFdSet);
					}
					/* Client initiating connection */
					else {
						printf("Incoming connection from client %s, port %hd\n", inet_ntoa(clientName.sin_addr), ntohs(clientName.sin_port));
						FD_SET(clientSocket, &activeFdSet);
					}
				}
				/* Incoming transaction from a client */
				else if (!FD_ISSET(i, &processingFdSet) && !FD_ISSET(i, &serverFdSet)) {
					t[thread_counter].thread_id=thread_counter;
					t[thread_counter].socketfd=i;
					FD_SET(i, &processingFdSet);
					j = readMessage(i, t[thread_counter].buffer);
					if (j<0) {
						perror("Error while trying to read data from client socket!\n");
						exit(-1);
					}
					writeMessage(i, "Transaction accepted, please wait...");
					pthread_create(&thread[(thread_counter++)%maxConn] , &attr, handle_client, (void *) &t[thread_counter]);
					thread_counter++;
					thread_counter%=maxConn;
				}
				/* Incoming transaction from another middleware */
				else if (FD_ISSET(i, &serverFdSet)) {
					t[thread_counter].thread_id=thread_counter;
					t[thread_counter].socketfd=clientSocket;
					j = readMessage(i, t[thread_counter].buffer);
					if( j<0 ) {
						perror("Error while trying to read data from middleware socket!\n");
						exit(-1);
					}
					pthread_create(&thread[thread_counter] , &attr, handle_middleware, (void *) &t[thread_counter]);
					thread_counter++;
					thread_counter%=maxConn;
					FD_CLR(i, &activeFdSet);
					FD_CLR(i, &serverFdSet);
				}
			}
		}
	}
	pthread_attr_destroy(&attr);
	pthread_exit(NULL);
}
