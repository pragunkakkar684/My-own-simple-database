#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <stdint.h>

typedef struct
{
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
} inputbuffer;

typedef enum
{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND,
} metacommandresult;

typedef enum
{
    PREPARE_SUCCESS,
    PREPARE_URECOGNISED_STATEMENT,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
} prepareresult;

typedef enum
{
    STATEMENT_INSERT,
    STATEMENT_SELECT
} statementtype;

typedef enum
{
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} executeresult;

typedef struct
{
    statementtype type;
    row row_to_insert; // to be used only by insert statement
} statement;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 225

typedef struct
{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
} row;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute) // a macro
const uint32_t ID_SIZE = size_of_attribute(row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET+ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET+USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE+USERNAME_SIZE+EMAIL_SIZE;
const uint32_t PAGE_SIZE = 4096;

#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE/ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE*TABLE_MAX_PAGES;

typedef struct{
    uint32_t num_rows;
    void* pages[TABLE_MAX_PAGES];
}table;

void print_row(row* row){
    printf("(%d, %s, %s)\n", row->id,row->username,row->email);
}

void serialize_row(row* source, void* destination){
    memcpy(destination+ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination+USERNAME_OFFSET, &(source->username),USERNAME_SIZE);
    memcpy(destination+EMAIL_OFFSET, &(source->email),EMAIL_SIZE);
}

void deserialize_row(void* source, row* destination){
    memcpy(&(destination->id), source+ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source+USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source+EMAIL_OFFSET, EMAIL_SIZE);
}

void* row_slot(table* table, uint32_t row_num){
    uint32_t page_num = row_num/ROWS_PER_PAGE;
    void* page = table->pages[page_num];
    if(page==NULL){
        //memory should be allcoated when we try ti accesspage
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_num %ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset*ROW_SIZE;
    return page+byte_offset;
}

table* new_table(){
    table* table_ptr = (table*)malloc(sizeof(table));
    table_ptr->num_rows = 0;
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        table_ptr->pages[i] = NULL;
    }
    return table_ptr;
}

void free_table(table* table){
    for(int i=0;table->pages[i];i++){
        free(table->pages[i]);
    }
    free(table);
}

inputbuffer *new_input_buffer()
{
    inputbuffer *input_buffer = malloc(sizeof(inputbuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

void print_prompt() { printf("Sup boy>"); }

void read_input(inputbuffer *input_buffer)
{
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

    if (bytes_read <= 0)
    {
        printf("Error reading Input\n");
        exit(EXIT_FAILURE);
    }

    // ignore traling newline
    input_buffer->input_length = bytes_read - 1; // this excludes newline from getting stored
    input_buffer->buffer[bytes_read - 1] = 0;    // replaces newline with \0
}

void close_input_buffer(inputbuffer *input_buffer)
{
    free(input_buffer->buffer);
    free(input_buffer);
}

metacommandresult do_meta_command(inputbuffer *input_buffer)
{
    if (strcmp(input_buffer->buffer, ".exit") == 0)
    {
        close_input_buffer(input_buffer);
        exit(EXIT_SUCCESS);
    }
    else
    {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

prepareresult prepare_statement(inputbuffer *input_buffer, statement *statement)
{
    if (strncmp(input_buffer->buffer, "insert", 6) == 0)
    {
        statement->type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }
    if (strcmp(input_buffer->buffer, "select") == 0)
    {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_URECOGNISED_STATEMENT;
}

void execute_statement(statement *statement)
{
    switch (statement->type)
    {
    case (STATEMENT_INSERT):
        printf("This is where we do an insert\n");
        break;
    case (STATEMENT_SELECT):
        printf("This is where we do a select\n");
        break;
    }
}

int main(int argc, char *argv[])
{
    inputbuffer *input_buffer = new_input_buffer();
    while (true)
    {
        print_prompt();
        read_input(input_buffer);

        if (input_buffer->buffer[0] == '.')
        {
            switch (do_meta_command(input_buffer))
            {
            case (META_COMMAND_SUCCESS):
                continue;
            case (META_COMMAND_UNRECOGNIZED_COMMAND):
                printf("Unrecognized command '%s' \n", input_buffer->buffer);
                continue;
            }
        }

        statement statement;
        switch (prepare_statement(input_buffer, &statement))
        {
        case PREPARE_SUCCESS:
            break;
        case PREPARE_URECOGNISED_STATEMENT:
            printf("Unrecognized statement at start of'%s'. \n", input_buffer->buffer);
            continue;
        }

        execute_statement(&statement);
        printf("Executed\n");
    }
}
