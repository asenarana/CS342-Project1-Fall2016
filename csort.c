/*
*  A. Rana Yozgatli
*  21000132
*  Section 2
*  CS342: Operating Systems
*  Project 1
*  
*  csort.c
*  Parameters: workerCount, inputFileName, outputFileName
*  outputFile = sort(inputFile)
*  The output file is sorted in ascending order.
*  Integers are sorted using multiple processes.
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>
#include <mqueue.h>

// node definition for the list
struct intNode
{
	int number;
	int binary;
	struct intNode *next;
};

// function prototypes
void zeChild( FILE *inFile, int index, int count, mqd_t cmid);
void parentingSucks(FILE *outFile, mqd_t mids[5], int pcount, int total);
int binToInt(int bin[8]);
void findPlace( struct intNode *head, int num);
int smallest( int array[], int count);
int fileSize( FILE *theFile);
//int processCount( int totalInt);

// ze main
int main( int argc, char *argv[] )
{
	// VARIABLES'N'STUFF
	FILE *input;
	FILE *output;
	
	int pCount;		// process count
	int iCount;		// integer per process
	int xCount;		// extra integer count for the last process
	int index;		// child will start reading from index
	int count;		// child will read this much int
	int size;		// file size
	int total;		// total int count
	
	pid_t pid;		// process id
	mqd_t pmid;		// message queue id for parent
	mqd_t cmid;		// message queue id for child
	
	mqd_t mids[5];		// message queue id array
	char *name;		// name of the message queue
	
	struct mq_attr attr;
	
	// CHECK THE INPUT
	// check the input count
	if( argc != 4)
	{
		printf( "Integer count, input file name, output file name are expected!\n");
		return -1;
	}
	// check the integer count
	pCount = atoi( argv[1]);
	if( pCount < 1 || pCount > 5)
	{
		printf( "Worker count must be between 1 - 5.\n");
		return -1;
	}
	// open and check the files
	input = fopen( argv[2], "rb");
	output = fopen( argv[3], "wb");
	if( input == NULL || output == NULL)
	{
		printf( "An error occurred while opening the file(s).\n");
		return -1;
	}
	
	// CALCULATIONS
	
	//find size
	size = fileSize( input);
	total = size / 8;
	xCount = total % pCount;
	iCount = (total - xCount) / pCount;
	
	// attributes for message queue
	attr.mq_flags = O_RDWR;
	attr.mq_maxmsg = xCount + iCount;
	attr.mq_msgsize = 8;
	attr.mq_curmsgs = 0;
	
	// MULTI-PROCESSING
	for( int i = 0; i < pCount; i++)
	{
		switch(i)
		{
			case 0:
				name = "/0";
				break;
			case 1:
				name = "/1";
				break;
			case 2:
				name = "/2";
				break;
			case 3:
				name = "/3";
				break;
			case 4:
				name = "/4";
				break;
				
		}
		// creating message queue
		mq_open(name, O_CREAT, (S_IRWXU | S_IRWXG | S_IRWXO), &attr);
		
		pid = fork();
		if( pid < 0)
		{
			// error: terminate process
			printf( "No fork for you.\n");
			exit(0);
		}
		else if( pid == 0)
		{
			// child
			cmid = mq_open( name, O_WRONLY);
			index = i * iCount;
			if( i == pCount -1)
			{
				// the last process
				count = iCount + xCount;
			}
			else
			{
				// a process
				count = iCount;
			}
			zeChild( input, index, count, cmid);
			exit(0);
		}
		else
		{
			//parent
			pmid = mq_open( name, O_RDONLY);
			mids[i] = pmid;
			// go create the other processes
		}
	}
	// wait for the results
	struct mq_attr *mqstat;
	mqstat = malloc( sizeof( struct mq_attr));
	for( int j = 0; j < pCount; j++)
	{
		while(1)
		{
			mq_getattr(mids[j], mqstat);
			if( mqstat -> mq_curmsgs > 0)
				break;
		}
	}
	// do the parenting
	parentingSucks( output, mids, pCount, total);
	// CLEAN-UP
	for( int k = 0; k < pCount; k++)
	{
		mq_close(mids[k]);
	}
	fclose(input);
	fclose(output);
	return 0;
}

void zeChild( FILE *inFile, int index, int count, mqd_t cmid)
{
	int theBin[8];
	int theInt;
	struct intNode *root;
	struct intNode *newNode;
	struct intNode *cur;
	// set the linked list
	root = malloc( sizeof( struct intNode));
	root -> next = NULL;
	cur = root;
	// go to index
	fseek( inFile, index, SEEK_SET);
	// bin to int to list
	for( int i = 0; i < count; i++)
	{
		fread(theBin, 1, 8, inFile);
		theInt = binToInt( theBin);
		newNode = malloc( sizeof( struct intNode));
		newNode -> number = theInt;
		
		cur = root;
		findPlace(cur, theInt);
		newNode -> next = cur -> next;
		cur -> next = newNode;
	}
	// send messages and clean-up
	cur = root -> next;
	for( int j = 0; j < count; j++)
	{
		// Note: newNode is acting like temp
		mq_send(cmid, (const char *) &cur, sizeof(cur), 0);
		newNode = cur -> next;
		free(cur);
		cur = newNode;
	}
	// send null
	cur = NULL;
	mq_send(cmid, (const char *) &cur, sizeof(cur), 0);
	return;
}

void parentingSucks(FILE *outFile, mqd_t mids[5], int pcount, int total)
{	
	struct intNode *temp;
	//struct intNode **curNode[pcount];
	int tempArray[pcount];
	int mindex;	// message index
	char str[4];
	
	// get the first messages
	for( int i = 0; i < pcount; i++)
	{
		mq_receive(mids[i], (char *) &temp, sizeof(temp), NULL);
		if (temp == NULL){
			//*curNode[i] = NULL;
			tempArray[i] = -1;
		}
		else
		{
			//*curNode[i] = temp;
			tempArray[i] = temp -> number;
		}
	}
	// outputting
	for( int j = 0; j < total; j++)
	{
		// find message index of smallest
		mindex = smallest( tempArray, pcount);
		if( mindex == -1)
		{
			break;
		}
		// get and write the integer
		
		//temp = *curNode[mindex];
		sprintf(str, "%i", tempArray[mindex]);
		fputs( (const char *) str, outFile);
		// get the next node
		mq_receive(mids[mindex], (char *) &temp, sizeof(temp), NULL);
		if (temp == NULL){
			//*curNode[mindex] = NULL;
			tempArray[mindex] = -1;
		}
		else
		{
			//*curNode[mindex] = temp;
			tempArray[mindex] = temp -> number;
		}
	}
	return;
}

int binToInt(int bin[8])
{
	int zeInt = 0;
	int temp = 0;
	for( int i=0; i<8; i++)
	{
		temp = bin[i] * pow( 2, 8-i);
		zeInt = zeInt + temp;
	}
	return zeInt;
}

void findPlace( struct intNode *head, int num)
{
	if( head -> next == NULL)
	{
		return;
	}
	while( head -> next != NULL)
	{
		if( head -> next -> number > num)
		{
			return;
		}
		head = head -> next;
	}
	return;
}

int smallest( int array[], int count)
{
	int number1;
	int number2;
	int index = -1;
	
	number1 = array[0];
	if( number1 != -1)
	{
		index = 0;
	}
	for( int i = 1; i < count; i++)
	{
		number2 = array[i];
		if(number2 != -1)
		{
			if( number2 < number1)
			{
				// switch
				number1 = number2;
				index = i;
			}
		}
	}
	return index;
}

int fileSize( FILE *theFile)
{
	int size;
	
	fseek( theFile, 0L, SEEK_END);
	size = ftell(theFile);
	fseek( theFile, 0L, SEEK_SET);
	return size;
}
