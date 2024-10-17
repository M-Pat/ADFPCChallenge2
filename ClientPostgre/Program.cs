using System;
using System.Collections.Generic;
using System.Data;
using Npgsql;

class Program
{
    static void Main(string[] args)
    {
        string connectionString = "Host=biennially-devoted-chimp.data-1.use1.tembo.io;Port=5432;Username=postgres;Password=8vcyLMSH8Zd89BBZ;Database=postgres";

        using (var connection = new NpgsqlConnection(connectionString))
        {
            connection.Open();
            Console.WriteLine("Connected to the database. Type your commands:");

            string command;
            while (true)
            {
                Console.Write("> ");
                command = Console.ReadLine();

                if (string.IsNullOrWhiteSpace(command))
                    continue;

                // Exit command
                if (command.Equals("exit", StringComparison.OrdinalIgnoreCase))
                    break;

                // Process commands
                if (command.StartsWith("\\t "))
                {
                    ShowTableDetails(connection, command.Substring(3));
                }
                else
                {
                    ExecuteQuery(connection, command);
                }

                if (command.Equals("\\tables", StringComparison.OrdinalIgnoreCase))
                {
                    ShowTableNames(connection);
                }

            }
        }

    }

    static void ShowTableDetails(NpgsqlConnection connection, string tableName)
    {
        string query = $@"
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = '{tableName}'";

        using (var cmd = new NpgsqlCommand(query, connection))
        using (var reader = cmd.ExecuteReader())
        {
            Console.WriteLine($"\nDetails for table: {tableName}");
            Console.WriteLine($"{"Column Name",-20} {"Data Type",-15} {"Is Nullable",-12} {"Default Value"}");

            while (reader.Read())
            {
                string columnName = reader.GetString(0);
                string dataType = reader.GetString(1);
                string isNullable = reader.GetString(2);
                string defaultValue = reader.IsDBNull(3) ? "NULL" : reader.GetString(3);

                Console.WriteLine($"{columnName,-20} {dataType,-15} {isNullable,-12} {defaultValue}");
            }
        }
    }

    static void ExecuteQuery(NpgsqlConnection connection, string query)
    {
        try
        {
            using (var cmd = new NpgsqlCommand(query, connection))
            {
                // Check if the query is a SELECT statement
                if (query.TrimStart().StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        PrintResultSet(reader);
                    }
                }
                else
                {
                    // For non-SELECT queries
                    int rowsAffected = cmd.ExecuteNonQuery();
                    Console.WriteLine(rowsAffected > 0 ? "Command executed successfully." : "No rows affected.");
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }

    static void PrintResultSet(NpgsqlDataReader reader)
    {
        var columnNames = new List<string>();
        for (int i = 0; i < reader.FieldCount; i++)
        {
            columnNames.Add(reader.GetName(i));
        }

        // Print column names
        Console.WriteLine(string.Join("\t", columnNames));

        // Print rows
        while (reader.Read())
        {
            var values = new List<string>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                values.Add(reader[i]?.ToString() ?? "NULL");
            }
            Console.WriteLine(string.Join("\t", values));
        }
    }

    static void ShowTableNames(NpgsqlConnection connection)
    {
        string query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";

        using (var cmd = new NpgsqlCommand(query, connection))
        using (var reader = cmd.ExecuteReader())
        {
            Console.WriteLine("\nTables in the database:");
            while (reader.Read())
            {
                Console.WriteLine(reader.GetString(0));
            }
        }
    }

}
