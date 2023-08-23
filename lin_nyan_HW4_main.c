/**************************************************************
* Class:  CSC-415-01 Spring 2023
* Name: Nyan Ye Lin
* Student ID: 921572181
* GitHub ID: yye99
* Project: Assignment 4 – Word Blast
*
* File: lin_nyan_HW4_main.c
*
* Description: Reading War and Peace text file using multiple threads.
*
**************************************************************/

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>



typedef struct node 
{
    int count;
    char word[50];
    struct node * next;
}node_t;

typedef struct 
{
    char * fileName;
    int chunkSize;
    int offset;
}ThreadData;



// function prototypes for linklist functions
void readFile(const char * fileName, char *buffer, size_t count, off_t offset);
void addNode(node_t *head,int count, char * word);
int findNode(node_t * head, char * target);
void updateCount(node_t * head, char * word);

node_t * head = NULL;       // global linkedlist 

void addNode(node_t *head,int count, char * word) 
{

    node_t * curr = head;
    
    while(curr->next != NULL) {
        curr = curr->next;
    }

    curr->next = (node_t *) malloc(sizeof(node_t));
    curr->next->count = count;
    strcpy(curr->next->word,word);
    curr->next->next = NULL;

}

int findNode(node_t * head, char * target) 
{
    node_t * curr = head;

    while(curr != NULL) 
    {
        if(strcmp(curr->word,target) == 0) 
        {
            return 0;
        }
        curr = curr->next;
    }
    return -1;
}

void updateCount(node_t * head,char * word) 
{
    node_t * curr = head;

    while(curr != NULL) 
    {
        if(strcmp(curr->word,word) == 0)
        {
            curr->count = curr->count + 1;
        }
        curr = curr->next;
    }
}


// You may find this Useful
char * delim = "/[#]\"\'.“”‘’?:;-,—*($%)! \t\n\x0A\r";

pthread_mutex_t mutex;


void *process(void *threadData) 
{
    // dereferening and casting into ThreadData
    ThreadData *data = (ThreadData *)threadData;

    char * fileName = data -> fileName;
    int chunkSize = data -> chunkSize;
    int offset = data -> offset;
    

    char buffer[chunkSize];
    
    readFile(fileName,buffer,chunkSize,offset);

    char *savedptr;
    char * token = strtok_r(buffer,delim,&savedptr);
    
    // mutex locks starts here
    pthread_mutex_lock(&mutex);

    node_t * newHead = (node_t*) malloc(sizeof(node_t));

    while(token != NULL) 
    {
        // -1 --> token does not exist in the list
        // 0 --> token exists in the list
        if(findNode(newHead,token) == -1 && strlen(token) >= 6)
        {
            addNode(newHead,1,token);
        }
        else if(findNode(newHead,token) == 0) 
        {
            updateCount(newHead,token);
        }
        
        token = strtok_r(NULL,delim,&savedptr);
    }

    // combining the list in the thread to
    // to the main list 

    if( head == NULL) 
    {
        head = newHead;
    }
    else 
    {
        while(newHead != NULL) 
        {
            // if word exist in the main list
            // update the count in the main list
            if(findNode(head,newHead->word) == 0)
            {
                updateCount(head,newHead->word);
            } 
            else   // otherwise, add to the main list 
            {
                addNode(head,1,newHead->word);
            }

            newHead = newHead->next;
        }

    }

    pthread_mutex_unlock(&mutex);
    // mutex lock ends here

    
}


void readFile(const char * fileName, char *buffer, size_t count, off_t offset) 
{
    int fd;
    fd = open(fileName,O_RDONLY);

    if(fd == -1) 
    {
        printf("Opening file failed..");
        exit(EXIT_FAILURE);
    }

    pread(fd,buffer,count,offset);

    close(fd);
}


long findFileSize(const char * fileName) 
{
    int fd;
    off_t size;

    fd = open(fileName,O_RDONLY);

    if(fd == -1) 
    {
        printf("Opening file failed..");
        exit(EXIT_FAILURE);
    }

    size = lseek(fd,0,SEEK_END);

    if(size == -1) 
    {
        printf("lseek failed..");
        exit(EXIT_FAILURE);
    }

    close(fd);

    return size;
}

// Insertion sort descending order
void insertionSort(node_t ** list) 
{
    node_t * sorted = NULL;
    node_t * curr = *list;

    while (curr != NULL)
    {
        node_t * next = curr -> next;
        if(sorted == NULL || sorted->count < curr->count)
        {
            curr->next = sorted;
            sorted = curr;
        }
        else 
        {
            node_t * temp = sorted;
            while(temp->next != NULL && temp->next->count > curr->count)
            {
                temp = temp->next;
            }
            curr->next = temp->next;
            temp->next = curr;
        }

        curr = next;
    }
    
    *list = sorted;
}



int main (int argc, char *argv[])
{
    //***TO DO***  Look at arguments, open file, divide by threads
    //             Allocate and Initialize and storage structures

    //**************************************************************
    // DO NOT CHANGE THIS BLOCK
    //Time stamp start
    struct timespec startTime;
    struct timespec endTime;

    clock_gettime(CLOCK_REALTIME, &startTime);
    //**************************************************************

    // Getting the file name and 
    // thread count from command line
    char * fileName = argv[1];
    // convert char into int 
    int threadCount = atoi(argv[2]);

    long fileSize = findFileSize(fileName);

    // printf("Thread Count : %d\n", threadCount);
    // printf("Size of File: %ld\n",fileSize);

    int chunkSize = fileSize / threadCount;
    int remainder = fileSize % threadCount;
    int lastChunk = chunkSize + remainder;

    // printf("Chunk Size : %d\n",chunkSize);
    // printf("Remainder : %d\n",remainder);

    // create an array of ThreadData
    // store file name, portion to process
    // and offset 
    ThreadData data[threadCount];

    for(int i = 0; i < threadCount; i++) 
    {
        // last thread will proccess lastChunk
        if(i == threadCount - 1) 
        {
            data[i].fileName = fileName;
            data[i].chunkSize = lastChunk;
            data[i].offset = i * chunkSize;
        }
        else 
        {
            data[i].fileName = fileName;
            data[i].chunkSize = chunkSize;
            data[i].offset = i * chunkSize;
        }
    }

    // threads are created here in a loop

    pthread_t threads[threadCount];

    // mutex initialization

    pthread_mutex_init(&mutex,NULL);

    for(int i = 0; i < threadCount; i++) 
    {
        pthread_create(&threads[i],NULL,process,(void*) &data[i]);

        pthread_join(threads[i],NULL);

    }

    // destroy mutex at the end
    pthread_mutex_destroy(&mutex);

    // sorting
    insertionSort(&head);

    int i = 1;
    node_t * curr = head;
    
    printf("\n");

    printf("Word Frequency Count on %s with %d threads\n",fileName,threadCount);
    printf("Printing top 10 words 6 characters or more\n");
    
    while(i < 11) {
        printf("Number %d is %s with a count of %d\n",i,curr->word,curr->count);
        curr = curr->next;
        i++;
    }


    //**************************************************************
    // DO NOT CHANGE THIS BLOCK
    // Clock output
    clock_gettime(CLOCK_REALTIME, &endTime);
    time_t sec = endTime.tv_sec - startTime.tv_sec;
    long n_sec = endTime.tv_nsec - startTime.tv_nsec;
    if (endTime.tv_nsec < startTime.tv_nsec)
        {
        --sec;
        n_sec = n_sec + 1000000000L;
        }

    printf("Total Time was %ld.%09ld seconds\n", sec, n_sec);
    //**************************************************************

}
