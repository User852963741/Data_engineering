using CsvHelper;
using Microsoft.Office.Interop.Access;
using System.Data;
using System.Formats.Asn1;
using System.Globalization;
using System.IO;
using System.Reflection.PortableExecutable;
using System.Text;

string dataFolder = @"C:\Users\ave\Desktop\data";
var clicksFolder = new DirectoryInfo($"{dataFolder}\\clicks").GetFiles();
var usersFolder = new DirectoryInfo($"{dataFolder}\\users").GetFiles();
string country = "LT"; // Country filter
//string country = null;

Dictionary<string, int> dailyClicks = new();
Dictionary<string, int> dataCollumn = new();
Dictionary<string, string> idToCountry = new();

if (country != null)
{
    foreach (var user in usersFolder)
    {
        using (var usersReader = new StreamReader(user.FullName))
        {
            int i = 0;
            var collumn = usersReader.ReadLine().Split(',');
            foreach (var item in collumn)
            {
                dataCollumn[item] = i;
                i++;
            }

            while (usersReader.EndOfStream == false)
            {
                string[] parts = usersReader.ReadLine().Split(',');
                var requeredPart = parts[dataCollumn["id"]];
                if (idToCountry.ContainsKey(requeredPart))
                {
                    continue;
                }
                else
                {
                    idToCountry[requeredPart] = parts[dataCollumn["country"]];
                }

            }
        }
    }

}

foreach (var click in clicksFolder)
{
    using (var reader = new StreamReader(click.FullName))
    {
        int i = 0;
        foreach (var item in reader.ReadLine().Split(','))
        {
            dataCollumn[item] = i;
            i++;
        }

        while (reader.EndOfStream == false)
        {
            string[] parts = reader.ReadLine().Split(',');
            var requiredPart = parts[dataCollumn["date"]];
            var userId = parts[dataCollumn["user_id"]];
            if (dailyClicks.ContainsKey(requiredPart) && (string.IsNullOrEmpty(country) || idToCountry.ContainsKey(userId) && idToCountry[userId] == country))
            {
                dailyClicks[requiredPart]++;
            }
            else
            {
                if (string.IsNullOrEmpty(country) || idToCountry.ContainsKey(userId) && idToCountry[userId] == country)
                {
                    dailyClicks[requiredPart] = 1;
                }
                else
                {
                    if (!dailyClicks.ContainsKey(requiredPart))
                    {
                        // If there are no clicks from the specified country that day it should still show up in the list with count 0
                        dailyClicks[requiredPart] = 0;
                    }
                }
            }

        }
    }
}

var path = string.IsNullOrEmpty(country) ? "total_clicks" : "filtered_clicks";

DataTable dt = new DataTable { TableName = path };
dt.Columns.Add("date");
dt.Columns.Add("count");

foreach (var pair in dailyClicks)
{
    dt.Rows.Add(pair.Key, pair.Value);
}

StringBuilder sb = new StringBuilder();

string[] columnNames = dt.Columns.Cast<DataColumn>().
                                  Select(column => column.ColumnName).
                                  ToArray();
sb.AppendLine(string.Join(",", columnNames));

foreach (DataRow row in dt.Rows)
{
    string[] fields = row.ItemArray.Select(field => field.ToString()).ToArray();
    sb.AppendLine(string.Join(",", fields));
}

File.WriteAllText($"{dataFolder}\\{path}.csv", sb.ToString());