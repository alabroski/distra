#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#define PORT 5555
#define hostNameLength 50
#define MAXMSG 512
#define STDIN 0

/* initSocketAddress
* Initialises a sockaddr_in struct given a host name and a port.
*/
void initSocketAddress(struct sockaddr_in *name, char *hostName, unsigned short int port)
{
	struct hostent *hostInfo; /* Contains info about the host */

	/* Socket address format set to AF_INET for internet use. */
	name->sin_family = AF_INET;

	/* Set port number. The function htons converts from host byte order to network byte order.*/
	name->sin_port = htons(port);

	/* Get info about host. */
	hostInfo = gethostbyname(hostName);
	if(hostInfo == NULL)
	{
		fprintf(stderr, "initSocketAddress - Unknown host %s\n",hostName);
		exit(EXIT_FAILURE);
	}
	/* Fill in the host name into the sockaddr_in struct. */
	name->sin_addr = *(struct in_addr *)hostInfo->h_addr;
}


/* writeMessage
* Writes the string message to the file (socket)
* denoted by fileDescriptor.
*/
void writeMessage(int fileDescriptor, char *message)
{
	int nOfBytes;

	nOfBytes = write(fileDescriptor, message, strlen(message) + 1);
	if(nOfBytes < 0)
	{
		perror("writeMessage - Could not write data\n");
		exit(EXIT_FAILURE);
	}
}
int readMessage(int fileDescriptor)
{
	int nOfBytes;
	char buffer[MAXMSG];
	nOfBytes = read(fileDescriptor, buffer, MAXMSG);
	if(nOfBytes < 0)
	{
        return 0;
	}
	printf("Message received from server: %s\n", buffer);
	return 1;
}

void choppy(char *a)
{
	int len;

	len = strlen(a);
	if( a[len-1] == '\n' )
    		a[len-1] = '\0';
}

int main(int argc, char *argv[])
{
	int sock, i;
	struct sockaddr_in serverName;
	char hostName[hostNameLength];
	char messageString[MAXMSG];
	fd_set activeFdSet, readFdSet;
	FILE *transaction;

	/* Check arguments */
	if(argv[1] == NULL)
	{
		perror("Usage: client [host name]\n");
		exit(EXIT_FAILURE);
	}
	else
	{
		strncpy(hostName, argv[1], hostNameLength);
		hostName[hostNameLength - 1] = '\0';
	}
	/* Create the socket */
	sock = socket(PF_INET, SOCK_STREAM, 0);
	if(sock < 0)
	{
		perror("Could not create a socket\n");
		exit(EXIT_FAILURE);
	}
	/* Initialise the socket address */
	initSocketAddress(&serverName, hostName, PORT);
	/* Connect to the server */
	if(connect(sock, (struct sockaddr *)&serverName, sizeof(serverName)) < 0)
	{
		perror("Could not connect to server\n");
		exit(EXIT_FAILURE);
	}
	FD_ZERO(&activeFdSet);
	FD_ZERO(&readFdSet);
	FD_SET(sock, &activeFdSet);
	FD_SET(STDIN, &activeFdSet);
	/* Send data to the server */
	printf("\nType a transaction file name to send to server:\n");
	printf("Type 'quit' to nuke this program.\n");
	fflush(stdin);

	while(1)
	{
		readFdSet=activeFdSet;
		if(select(FD_SETSIZE, &readFdSet, NULL, NULL, NULL) <= 0)
		{
			perror("Select failed\n");
			continue;
		}
		for(i = 0; i < FD_SETSIZE; ++i)
		{
			if(FD_ISSET(i, &readFdSet))
			{
				if(i==STDIN)
				{
					fgets(messageString, MAXMSG, stdin);
					messageString[MAXMSG - 1] = '\0';
					if(strncmp(messageString, "quit\n", MAXMSG))
					{
						choppy(messageString);
						transaction = fopen(messageString, "r");
						fread(messageString, MAXMSG, 1, transaction);
						writeMessage(sock, messageString);
					}
					else
					{
						close(sock);
						exit(EXIT_SUCCESS);
					}
					fflush(stdin);
				}
				else if(i==sock)
				{
					if(!(readMessage(sock)))
					{
                        perror("Connection closed by server!\n");
                        exit(EXIT_FAILURE);
					}
				}
			}
		}
	}
}
