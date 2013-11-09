#ifndef PAXTYPES_H
#define PAXTYPES_H

#include<stdio.h>
#include <sys/socket.h> 
#include <netdb.h> 
#include <netinet/in.h>
#include <arpa/inet.h> 
#include <stdio.h> 
#include <unistd.h> 
#include <string.h> 
#include <errno.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/time.h>
#include<stdlib.h>

#define DEBUG 1
#define USERINPUT 1

#define COMMAND_FILE_PREFIX "command_list_"
#define RESOURCE_FILE_PREFIX "rep"
#define FILENAME_LENGTH 100
#define COMMAND_LENGTH 1000

 
#define DELIMITER ":"
#define DELIMITER_SEC ";"
#define DELIMITER_CMD "-"
#define MAX_CLIENTS 2
#define MAX_ACCEPTORS 3
#define MAX_LEADERS 2
#define MAX_REPLICAS 2
#define MAX_COMMANDERS 1000
#define MAX_SCOUTS 1000
#define MAX_COMMANDERS_PER_LEADER 500
#define MAX_SCOUTS_PER_LEADER 500

#define COMMANDER_PORT_STARTER 5000
#define SCOUT_PORT_STARTER 6000

#define LISTENER_INDEX 0
#define TALKER_INDEX 1

#define BUFSIZE 5000

#define BALLOT_STRING_PREP(STR,BALLOT) sprintf(STR,"%d",BALLOT.bnum); \
					strcat(STR,DELIMITER_SEC);  \
					sprintf(STR,"%s%d",STR,BALLOT.leader_id); \
					strcat(STR,DELIMITER_SEC);

#define GET_NEXT_CMD_ID command_counter*MAX_CLIENTS+my_pid 

#define MAX_SET_SIZE 100
#define MAX_SLOTS 100
#define MAX_ACC_NAME_LENGTH 200
#define MAX_COMMANDS 100

enum COMMAND_TYPE
{
	COMMAND_DEPOSIT=0,
	COMMAND_WITHDRAW=1,
	COMMAND_ACCBALANCE=2   //READ ONLY
};
struct COMMAND_ID 
{
	int client_id;
	int cmd_seq_num;
};
struct COMMAND_ITEM
{
	int command_id;
	enum COMMAND_TYPE command_type;
	char command_data[BUFSIZE];
};
struct COMM_DATA
{

	//index 0 listener
	//index 1 talker
	int comm_port[2];
	int comm_fd[2];
};

struct BALLOT_NUMBER
{
	int bnum;
	int leader_id;
};
struct PROPOSAL
{
	int command[MAX_SLOTS]; //command[i] contains the command for slot 'i'
};
struct PVAL
{
	struct BALLOT_NUMBER ballot;
	int slot_number;
	int command;	//temporarily considered as int
};

struct COMMANDER_THREAD_ARG
{
	int parent_id;
	int my_pid;	
	struct PVAL my_pval;
};
struct SCOUT_THREAD_ARG
{
	int parent_id;
	int my_pid;	
	struct BALLOT_NUMBER my_ballot;
};
struct CLIENT_THREAD_ARG
{
	int my_pid;
};
#endif
