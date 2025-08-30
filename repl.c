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
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG
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

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct
{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} row;

typedef struct
{
    statementtype type;
    row row_to_insert;
} statement;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

#define ID_SIZE size_of_attribute(row, id)
#define USERNAME_SIZE size_of_attribute(row, username)
#define EMAIL_SIZE size_of_attribute(row, email)

#define ID_OFFSET 0
#define USERNAME_OFFSET (ID_OFFSET + ID_SIZE)
#define EMAIL_OFFSET (USERNAME_OFFSET + USERNAME_SIZE)
#define ROW_SIZE (ID_SIZE + USERNAME_SIZE + EMAIL_SIZE)

#define PAGE_SIZE 4096
#define TABLE_MAX_PAGES 100
#define ROWS_PER_PAGE (PAGE_SIZE / ROW_SIZE)
#define TABLE_MAX_ROWS (ROWS_PER_PAGE * TABLE_MAX_PAGES)

typedef struct
{
    uint32_t num_rows;
    void *pages[TABLE_MAX_PAGES];
} table;

void print_row(row *row)
{
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void serialize_row(row *source, void *destination)
{
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void *source, row *destination)
{
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void *row_slot(table *table, uint32_t row_num)
{
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = table->pages[page_num];
    if (page == NULL)
    {
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

table *new_table()
{
    table *table_ptr = (table *)malloc(sizeof(table));
    table_ptr->num_rows = 0;
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        table_ptr->pages[i] = NULL;
    }
    return table_ptr;
}

void free_table(table *table)
{
    for (int i = 0; table->pages[i]; i++)
    {
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

    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(inputbuffer *input_buffer)
{
    free(input_buffer->buffer);
    free(input_buffer);
}

metacommandresult do_meta_command(inputbuffer *input_buffer, table *table)
{
    if (strcmp(input_buffer->buffer, ".exit") == 0)
    {
        close_input_buffer(input_buffer);
        free_table(table);
        exit(EXIT_SUCCESS);
    }
    else
    {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

prepareresult prepare_insert(inputbuffer *input_buffer, statement *statement)
{
    statement->type = STATEMENT_INSERT;

    char *keyword = strtok(input_buffer->buffer, " ");
    char *id_string = strtok(NULL, " ");
    char *username = strtok(NULL, " ");
    char *email = strtok(NULL, " ");

    if (id_string == NULL || username == NULL || email == NULL)
    {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_string);
    if (id < 0)
        return PREPARE_NEGATIVE_ID;
    if (strlen(username) > COLUMN_USERNAME_SIZE)
        return PREPARE_STRING_TOO_LONG;
    if (strlen(email) > COLUMN_EMAIL_SIZE)
        return PREPARE_STRING_TOO_LONG;

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}

prepareresult prepare_statement(inputbuffer *input_buffer, statement *statement)
{
    if (strncmp(input_buffer->buffer, "insert", 6) == 0)
    {
        return prepare_insert(input_buffer, statement);
    }
    if (strcmp(input_buffer->buffer, "select") == 0)
    {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_URECOGNISED_STATEMENT;
}

// ----------- EXECUTION -----------
executeresult execute_select(statement *statement, table *table)
{
    row row;
    for (uint32_t i = 0; i < table->num_rows; i++)
    {
        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

executeresult execute_insert(statement *statement, table *table)
{
    if (table->num_rows >= TABLE_MAX_ROWS)
    {
        return EXECUTE_TABLE_FULL;
    }

    row *row_to_insert = &(statement->row_to_insert);
    serialize_row(row_to_insert, row_slot(table, table->num_rows));
    table->num_rows += 1;
    return EXECUTE_SUCCESS;
}

executeresult execute_statement(statement *statement, table *table)
{
    switch (statement->type)
    {
    case STATEMENT_INSERT:
        return execute_insert(statement, table);
    case STATEMENT_SELECT:
        return execute_select(statement, table);
    }
}

int main(int argc, char *argv[])
{
    table *table = new_table();
    inputbuffer *input_buffer = new_input_buffer();

    while (true)
    {
        print_prompt();
        read_input(input_buffer);

        if (input_buffer->buffer[0] == '.')
        {
            switch (do_meta_command(input_buffer, table))
            {
            case META_COMMAND_SUCCESS:
                continue;
            case META_COMMAND_UNRECOGNIZED_COMMAND:
                printf("Unrecognized command '%s'\n", input_buffer->buffer);
                continue;
            }
        }

        statement statement;
        switch (prepare_statement(input_buffer, &statement))
        {
        case PREPARE_SUCCESS:
            break;
        case PREPARE_SYNTAX_ERROR:
            printf("Syntax error. Could not parse statement.\n");
            continue;
        case PREPARE_URECOGNISED_STATEMENT:
            printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
            continue;
        case PREPARE_NEGATIVE_ID:
            printf("ID must be positive.\n");
            continue;
        case PREPARE_STRING_TOO_LONG:
            printf("String is too long.\n");
            continue;
        }

        switch (execute_statement(&statement, table))
        {
        case EXECUTE_SUCCESS:
            printf("Executed.\n");
            break;
        case EXECUTE_TABLE_FULL:
            printf("Error: Table full.\n");
            break;
        }
    }
}
