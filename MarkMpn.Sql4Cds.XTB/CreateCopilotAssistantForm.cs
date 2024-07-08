using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using OpenAI.Assistants;

namespace MarkMpn.Sql4Cds.XTB
{
    public partial class CreateCopilotAssistantForm : Form
    {
        private readonly string _endpoint;
        private readonly string _apiKey;

        public CreateCopilotAssistantForm(string endpoint, string apiKey)
        {
            InitializeComponent();

            _endpoint = endpoint;
            _apiKey = apiKey;
        }

        public string AssistantId { get; private set; }

        private void okButton_Click(object sender, EventArgs e)
        {
            progressPictureBox.Visible = true;
            backgroundWorker1.RunWorkerAsync();
        }

        private void backgroundWorker1_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            progressPictureBox.Visible = false;

            if (e.Error != null)
            {
                MessageBox.Show(this, e.Error.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
                return;
            }

            AssistantId = (string)e.Result;
            DialogResult = DialogResult.OK;
        }

        private void backgroundWorker1_DoWork(object sender, DoWorkEventArgs e)
        {
            var client = String.IsNullOrEmpty(_endpoint) ? new OpenAI.OpenAIClient(new System.ClientModel.ApiKeyCredential(_apiKey)) : new Azure.AI.OpenAI.AzureOpenAIClient(new Uri(_endpoint), new Azure.AzureKeyCredential(_apiKey));
#pragma warning disable OPENAI001 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.
            var assistantClient = client.GetAssistantClient();
#pragma warning restore OPENAI001 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.

            Assistant created = assistantClient.CreateAssistant(modelNameTextBox.Text, Definition);
            e.Result = created.Id;
        }

        public static AssistantCreationOptions Definition
        {
            get
            {
                string instructions;

                using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("MarkMpn.Sql4Cds.XTB.Resources.CopilotInstructions.txt"))
                using (var reader = new StreamReader(stream))
                {
                    instructions = reader.ReadToEnd();
                }

                return new AssistantCreationOptions
                {
                    Name = "SQL 4 CDS Copilot",
                    Description = "An AI assistant to help you write SQL queries in SQL 4 CDS",
                    Instructions = instructions,
                    Tools =
                    {
                        new FunctionToolDefinition
                        {
                            FunctionName = "list_tables",
                            Description = "Get the list of tables in the current environment",
                            Parameters = BinaryData.FromObjectAsJson(new { type = "object", properties = new { } })
                        },
                        new FunctionToolDefinition
                        {
                            FunctionName = "get_columns_in_table",
                            Description = "Get the list of columns in a table, including the available values for optionset columns and the relationships for foreign key columns",
                            Parameters = BinaryData.FromObjectAsJson(new {
                                type = "object",
                                properties = new {
                                    table_name = new {
                                        type = "string",
                                        description = "The name of the table, e.g. 'account'"
                                    }
                                },
                                required = new [] { "table_name" }
                            })
                        },
                        new FunctionToolDefinition
                        {
                            FunctionName = "get_current_query",
                            Description = "Gets the contents of the query the user is currently editing",
                            Parameters = BinaryData.FromObjectAsJson(new { type = "object", properties = new { } })
                        },
                        new FunctionToolDefinition
                        {
                            FunctionName = "execute_query",
                            Description = "Runs a SQL query and returns the result. Only queries which have previously been shown to the user will be run",
                            Parameters = BinaryData.FromObjectAsJson(new {
                                type = "object",
                                properties = new {
                                    query = new {
                                        type = "string",
                                        description = "The SQL query to execute"
                                    }
                                },
                                required = new [] { "query" }
                            })
                        },
                        new FunctionToolDefinition
                        {
                            FunctionName = "find_relationship",
                            Description = "Finds the path of joins that can be used to connect two tables",
                            Parameters = BinaryData.FromObjectAsJson(new {
                                type = "object",
                                properties = new {
                                    table1 = new {
                                        type = "string",
                                        description = "The table to join from"
                                    },
                                    table2 = new {
                                        type = "string",
                                        description = "The table to join to"
                                    }
                                },
                                required = new [] { "table1", "table2" }
                            })
                        }
                    }
                };
            }
        }
    }
}
