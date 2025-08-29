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
} statement;

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
