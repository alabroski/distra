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
#include "db_serv.h"


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
	if(sock < 0) {
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
	if(bind(sock, (struct sockaddr *)&name, sizeof(name)) < 0) {
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
	if(hostInfo == NULL) {
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
	if(nOfBytes < 0) {
		perror("Could not read data from client\n");
		return (-2);
	}
	else if(nOfBytes == 0)
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

/* Splits a given transaction into single operations.

Parameters:
char *transaction - the transaction string to be split
char *transaction_operations[maxOperationLength] - the string array to place the different operations in*/
int split_transaction(char *transaction, char transaction_operations[][maxOperationLength]) {
	int i, j, a;
	j=0;
	a=0;
	for (i=0; i < strlen(transaction); i++) {
		if (transaction[i] != '\n') {
			transaction_operations[j][a++] = transaction[i];
		}
		else {
			transaction_operations[j][a] = '\0';
			j++; a = 0;
		}
		if (i == (strlen(transaction)-1)) {	//Add null terminator to the last operation
			transaction_operations[j][a] = '\0';
		}
		if (j == maxTransOp) {
		
			return -1;
		}
	}
	return j;
}

/* Splits transaction operations into operands

Parameters:
char *operation - the single operation string
char *operands[] - the string array that will hold the operands */
void split_operation(char *operation, char operands[][maxOperationLength])
{
	char temp_operation[maxOperationLength];
	char *token;
	strcpy(temp_operation, operation);
	
	token = strtok(temp_operation, " ");
	strcpy(operands[0], token);
	
	token = strtok(NULL, " ");
	strcpy(operands[1], token);
	
	token = strtok(NULL, " ");
	strcpy(operands[2], token);
	
	if (strcmp(operands[0], "ASSIGN")) {
		token = strtok(NULL, " ");
		strcpy(operands[3], token);
	}
}

/* Releases acquired locks

Parameters:
int *lockedVariables - the local thread mutex array pointer */
void release_locks(int *lockedVariables) {
	int j;
	for(j=0; j<256; j++) {	//Releasing variable locks
		if(lockedVariables[j]) {
			printf("Released lock for %c!\n", (char)j);
			lockedVariables[j] = 0;
			dbmutex[j] = 0;
		}
	}
}

/* Thread handle for incoming transaction from a middleware */
void * handle(void * args)
{
	int operationsNumber, flag, flag_value;
	int trans_operand1, trans_operand2, trans_operand3;
	int i, j, printCount;
	int timesRetried, timesToRetry;
	char controlMsgs[MAXMSG];
	char hostName[hostNameLength];
	char operands[4][maxOperationLength];
	char transactionOperations[maxTransOp][hostNameLength];
	char printQueue[maxTransOp][hostNameLength];
	int trans_cache[256];
	int lockedVariables[256];
	struct thread_data t;
	struct thread_data *temp;
	FILE *dbfile; 		/* File pointer to database for commit operation */
	fd_set tempFdSet, readFdSet;

	/* Initiation of variables */
	temp = (struct thread_data *) args;
	t.thread_id = temp->thread_id;
	t.socketfd = temp->socketfd;
	strncpy(t.buffer, temp->buffer, MAXMSG);
	printCount = 0;
	timesRetried = 0;
	timesToRetry = ( (rand()%11)+5 );
	for (i=0; i<256; i++) {
		trans_cache[i] = -1;
		lockedVariables[i] = 0;
	}
	/* lockedVariables - which variables have already been locked for use by this particular transaction */

	/* Splitting transaction operations into multiple strings */
	operationsNumber = split_transaction(t.buffer, transactionOperations);

	if (operationsNumber == -1) {
		printf("Invalid transaction - cannot split operations!\n");
		writeMessage(t.socketfd, "0");
		goto answer;
	}
	printf("Number of operations: %d\n", operationsNumber);
	printf("Retry times = %d\n", timesToRetry);
	/* Mutex control*/
	while(1) {	/* Try to get all mutexes as many times as needed */
		printf("Transaction start!\n");
		flag = 1;
		for(i=0; i<operationsNumber; i++) {
			split_operation(transactionOperations[i], operands);
			/* ASSIGN transaction operation parsing */
			if (!strcmp(operands[0],"ASSIGN")) {
				if (strlen(operands[1])>1) {	//If the variable is not 1 character long - error
					perror("Transaction discarded: faulty first operand (ASSIGN)!\n");
					release_locks(lockedVariables);
					writeMessage(t.socketfd, "0");
					goto answer;
				}
				trans_operand1 = (int)operands[1][0];	//Geting the variable to assign a value to
				if (dbmutex[trans_operand1] && !lockedVariables[trans_operand1]) {
					/*Checking if the variable has already been previously locked and if we already have the lock*/
					flag = 0; //flag=0 means retry/abort
				}
				else if (!lockedVariables[trans_operand1]) {
					printf("Acquired lock for %c!\n", (char)trans_operand1);
					dbmutex[trans_operand1] = 1;	//Set to 1 to acknowledge that we will be using this DB variable
					lockedVariables[trans_operand1] = 1;
					if (!isdigit(operands[2][0])) {		//The second operand has to be a numeric value
						perror("Transaction discarded: faulty second operand (ASSIGN)!\n");
						release_locks(lockedVariables);
						writeMessage(t.socketfd, "0");
						goto answer;
					}
					trans_cache[trans_operand1] = database[trans_operand1];	//Setting the value in the local db cache
				}
			}

			/* ADD transaction operation parsing */
			else if (!strcmp(operands[0],"ADD")) {
				/* Handling of first operand */
				if (strlen(operands[1]) > 1) { 		//If the variable is not 1 character long - error
					perror("Transaction discarded: faulty first operand (ADD)!\n");
					release_locks(lockedVariables);
					writeMessage(t.socketfd, "0");
					goto answer;
				}
				trans_operand1 = (int)operands[1][0];
				if (dbmutex[trans_operand1] && !lockedVariables[trans_operand1]) {
					/*Checking if the variable has already been previously locked and if we already have the lock*/
					flag = 0;		//flag=0 means retry/abort
				}
				else if (!lockedVariables[trans_operand1]) {	//We have not previously acquired the global mutex for this variable
					printf("Acquired lock for %c!\n", (char)trans_operand1);
					dbmutex[trans_operand1] = 1;	//Set mutex to 1 to acknowledge that we will be using it
					lockedVariables[trans_operand1] = 1;
					trans_cache[trans_operand1] = database[trans_operand1];	//Set the global value in the local transaction cache
				}
				/* End of first operand handling */

				/* Handling of second operand */
				if ((strlen(operands[2])==1) && isalpha(operands[2][0]) && flag) {		//If the second operand is a variable
					trans_operand2 = (int)operands[2][0];
					if (dbmutex[trans_operand2] && !lockedVariables[trans_operand2]) {
						/*Checking if the variable has already been previously locked and if we already have the lock*/
						flag = 0;		//flag=0 means retry/abort
					}
					else if (!lockedVariables[trans_operand2])	//We have not previously acquired the global mutex for this variable
					{
						printf("Acquired lock for %c!\n", (char)trans_operand2);
						dbmutex[trans_operand2] = 1;	//Set mutex to 1 to acknowledge that we will be using it
						lockedVariables[trans_operand2] = 1;
						trans_cache[trans_operand2] = database[trans_operand2]; //Set the global value in the local transaction cache
					}
				}
				else if (!isdigit(operands[2][0])) {	//The second operand is neither a numeric value nor a variable - error
					perror("Transaction discarded: faulty second operand (ADD)!\n");
					release_locks(lockedVariables);
					writeMessage(t.socketfd, "0");
					goto answer;
				}
				/* End of handling of second operand */

				/* Handling of third operand */
				if ((strlen(operands[3])==1) && (isalpha(operands[3][0])) && (flag)) {	//If the third operand is a variable
					trans_operand3 = (int)operands[3][0];
					if (dbmutex[trans_operand3] && !lockedVariables[trans_operand3]) {
						/*Checking if the variable has already been previously locked and if we already have the lock*/
						flag = 0;	//flag=0 means retry/abort
					}
					else if (!lockedVariables[trans_operand3]) {
						printf("Acquired lock for %c!\n", (char)trans_operand3);
						/* Nobody has locked it and we don't have the lock - acquire it */
						dbmutex[trans_operand3] = 1;	//Set mutex to 1 to acknowledge that we will be using it
						lockedVariables[trans_operand3] = 1;
						trans_cache[trans_operand3] = database[trans_operand3];
					}
				}
				else if (!isdigit(operands[3][0])) { //The third operand is neither a numeric value nor a variable
					perror("Transaction discarded: faulty third operand (ADD)!\n");
					release_locks(lockedVariables);
					writeMessage(t.socketfd, "0");
					goto answer;
				}
				/* End of handling of third operand */
			}

			/* PRINT transaction operation parsing */
			else if (!(strcmp(operands[0],"PRINT"))) {
				if (strlen(operands[1])>1 || !isalpha(operands[1][0])) {			//If the variable is not 1 character long - error
					perror("Transaction discarded: faulty operand (PRINT)!\n");
					release_locks(lockedVariables);
					writeMessage(t.socketfd, "0");
					goto answer;
				}
				else {
					trans_operand1 = (int)operands[1][0];	//Geting the variable to print
					if ( (dbmutex[trans_operand1]) && (!(lockedVariables[trans_operand1])) ) {
						/*Checking if the variable has already been previously locked and if we already have the lock*/
						flag = 0;		//flag=0 means retry/abort
					}
					else if (!lockedVariables[trans_operand1]) {
						dbmutex[trans_operand1] = 1;	//Set to 1 to acknowledge that we will be using this DB variable
						lockedVariables[trans_operand1] = 1;
						trans_cache[trans_operand1] = database[trans_operand1];
					}
				}
			}

			/* If any of the locks haven't been acquired, abort/timesRetried */
			if (!flag) {
				release_locks(lockedVariables);
				timesRetried++;
				if (timesRetried == timesToRetry) {
					printf("Retried for %d - sending abort to middleware!\n", timesRetried);
					writeMessage(t.socketfd, "0");
					goto answer;
				}
				break;
			}
		}
		if (!flag) {	//Sleep for a random period of time before retrying
			srand(time(NULL));
			printf("Aborting transaction (RETRY)!\n");
			sleep( (rand()%7)+3 );
		}
		else
			break;
	}
	/* End of mutex control */

	sleep(6);
	/* Send answer to middleware and parse operations */
	if (flag) {
        printf("All locks acquired!\n");
		writeMessage(t.socketfd, "1");
		for (i=0; i<operationsNumber; i++) {
			split_operation(transactionOperations[i], operands);
			
			/* ASSIGN transaction operation parsing */
			if (!(strcmp(operands[0],"ASSIGN"))) {
				trans_operand1 = (int)operands[1][0];
				trans_operand2 = atoi(operands[2]);
				trans_cache[trans_operand1] = trans_operand2;	//Setting the value in the local db cache
			}

			/* ADD transaction operation parsing */
			else if (!(strcmp(operands[0],"ADD"))) {
				flag_value=0;
				/* Handling of first operand */
				trans_operand1 = (int)operands[1][0];
				/* End of first operand handling */

				/* Handling of second operand */
				if (isdigit(operands[2][0])) {	//If the second operand is a numeric value
					trans_operand2 = atoi(operands[2]);
					flag_value = 1;
				}
				else					//If the second operand is a variable
					trans_operand2 = (int)operands[2][0];
				/* End of handling of second operand */

				/* Handling of third operand */
				if (isdigit(operands[3][0]) ) {
					trans_operand3 = atoi(operands[3]);
					if (flag_value) {	//Third - value; Second - value
						trans_cache[trans_operand1] = trans_operand2 + trans_operand3;
					}
					else {	 			//Third - value; Second - variable
						if(trans_operand1==trans_operand2)
							trans_cache[trans_operand1] += trans_operand3;
						else
							trans_cache[trans_operand1] = trans_cache[trans_operand2] + trans_operand3;
					}
				}
				else {	//If the third operand is a variable
					trans_operand3 = (int)operands[3][0];
					if (!flag_value) {			//Third - variable; Second - variable
						trans_cache[trans_operand1] = trans_cache[trans_operand2] + trans_cache[trans_operand3];
					}
					else if (flag_value) {		//Third - variable; Second - value
						trans_cache[trans_operand1] = trans_operand2 + trans_cache[trans_operand3];
					}
				}
				/* End of handling of third operand */
			}

			/* PRINT transaction operation parsing */
			else if (!(strcmp(operands[0],"PRINT"))) {
				trans_operand1 = (int)operands[1][0];	//Geting the variable to print
				sprintf(printQueue[printCount++], "%c = %d\n", operands[1][0], trans_cache[trans_operand1]);
			}
			/* SLEEP transaction operation parsing */
			else if (!(strcmp(operands[0],"SLEEP"))) {
				/* ---TODO--- */
			}
		}
	}

answer:
	/* Receiving answer on what to do from coordinator */
	FD_ZERO(&tempFdSet);
	FD_SET(t.socketfd, &tempFdSet);
    readFdSet = tempFdSet;
	while ((select(FD_SETSIZE, &readFdSet, NULL, NULL, NULL)) <= 0) {
		perror("Select failed\n");
		readFdSet = tempFdSet;
		continue;
	}
	if(FD_ISSET(t.socketfd, &readFdSet)) {
		j = readMessage(t.socketfd, controlMsgs);
	}
	else {	//Select unblocked due to unknown reasons
		perror("Aborting transaction! (Select unblocked)\n");
		for(i=0; i<256; i++) {		//Releasing variable locks
			if(lockedVariables[i])
				dbmutex[i] = 0;
		}
		close(t.socketfd);
		pthread_exit(NULL);
	}
	/* Answer receiving end */

	/* Checking answer */
	if(j < 0 || (controlMsgs[0] == '0')) {	//Answer received - abort
		perror("Aborting transaction! (Checking answer)\n");
		for(i=0; i<256; i++) {		//Releasing variable locks
			if(lockedVariables[i])
				dbmutex[i] = 0;
		}
		close(t.socketfd);
		pthread_exit(NULL);
	}
	else if(controlMsgs[0] == '1') {	//Answer received - commit
		/* Committing transaction to RAM memory database */
		for(i=0; i<256; i++) {
			if(lockedVariables[i]) {
				database[i] = trans_cache[i];
				printf("COMMMIT: %c = %d\n", (char)i, database[i]);
			}
		}
		/* End of transaction commit to RAM */

		/* Start of transaction commit to physical file */
		dbfile = fopen("database", "w");
		if (!dbfile) {
			perror("Failed to open database file!\n Transaction commited only to RAM!\n");
			close(t.socketfd);
			pthread_exit(NULL);
		}
		for (i=0; i<256; i++) {
			if (database[i] != -1) {
				sprintf(controlMsgs, "%c %d\n", (char)i,  database[i]);
				fputs(controlMsgs, dbfile);
			}
		}
		for (i=0; i<256; i++) {		//Releasing variable locks
			if(lockedVariables[i])
				dbmutex[i] = 0;
		}
		fclose(dbfile);
		/* End of transaction commit to physical file */
	}
	close(t.socketfd);
	pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
	int sock, clientSocket; 		/* Incoming connections (sock) and communication initialization (clientSocket) */
	int i, j;
	char hostName[hostNameLength];		/* Temporary string used to keep IP addresses */
	struct sockaddr_in clientName;		/* Temporary address structs used during connection initialization*/
	size_t size;
	fd_set activeFdSet, readFdSet; 		/* Used by select */

	/* Thread declarations and init */
	pthread_t thread[maxConn];
	pthread_attr_t attr;
	int thread_counter;				/* Which thread is next to be used */
	struct thread_data t[maxConn];	/* Data structure to pass to thread handler */
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	/* End of thread declarations */

	srand(time(NULL));
	for(i=0; i<256; i++) {
		database[i] = -1;
		dbmutex[i] = 0;
	}
	j = thread_counter = 0;
	/* Create a socket and set it up to accept connections */
	sock = makeSocket(PORT);
	/* Listen for connection requests from clients */
	if(listen(sock,3) < 0) {
		perror("Could not listen for connections\n");
		exit(EXIT_FAILURE);
	}
	/* Initialise the set of active sockets */
	FD_ZERO(&activeFdSet);
	FD_ZERO(&readFdSet);
	FD_SET(sock, &activeFdSet);
	printf("Listening for connections...\n");

	while(1) {
		/* Block until input arrives on one or more active sockets
		FD_SETSIZE is a constant with value = 1024 */
		readFdSet = activeFdSet;
		if(select(FD_SETSIZE, &readFdSet, NULL, NULL, NULL) <= 0) {
			perror("Select failed\n");
			continue;
		}

		/* Service all the sockets with input pending */
		for(i = 0; i < FD_SETSIZE; ++i) {
			if(FD_ISSET(i, &readFdSet)) {
				/* Incoming connection on original socket */
				if(i == sock) {
					size = sizeof(clientName);
					clientSocket = accept(sock, (struct sockaddr *)&clientName, &size);
					if(clientSocket < 0) {
						perror("Could not accept connection\n");
						exit(EXIT_FAILURE);
					}
					strcpy(hostName,inet_ntoa(clientName.sin_addr));
					printf("Incoming connection from middleware %s, port %hd\n", inet_ntoa(clientName.sin_addr), ntohs(clientName.sin_port));
					FD_SET(clientSocket, &activeFdSet);
				}
				else if (FD_ISSET(i, &activeFdSet)) {
					t[thread_counter].thread_id=thread_counter;
					t[thread_counter].socketfd=i;
					j = readMessage(i, t[thread_counter].buffer);
					if (j<0) {
						perror("Error while trying to read data from middleware socket!\n");
						exit(-1);
					}
					pthread_create(&thread[thread_counter] , &attr, handle, (void *) &t[thread_counter]);
					thread_counter++;
					thread_counter%=maxConn;
					FD_CLR(i, &activeFdSet);
				}
			}
		}
	}
	pthread_attr_destroy(&attr);
	pthread_exit(NULL);
}
