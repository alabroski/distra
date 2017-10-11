/*
 * db_serv.h
 *
 *  Created on: 21.9.2014
 *      Author: Aleksandar
 */

#ifndef DB_SERV_H_
#define DB_SERV_H_

#define PORT 7777
#define MAXMSG 512
#define maxConn 20
#define hostNameLength 50
#define maxOperationLength 50
#define maxTransOp 25

/**** Declaration of global variables and structures ****/
char serverConn[maxConn][hostNameLength];			/* Keeps track of other middlewares' IP addresses */
int conn_count;				/* conn_count - how many other middlewares are there */
int dbmutex[256];			/* Symbolic mutex to keep track of access to database variables */
int database[256];			/* Local memory copy of the database, everything is saved here prior to commiting*/
struct thread_data
{
	int  thread_id;
	int  socketfd;
	char buffer[MAXMSG];
};
/**** End of declaration ****/



#endif /* DB_SERV_H_ */
