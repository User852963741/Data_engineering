using System.Data;
using System.Text;

string dataFolder = @"C:\Users\ave\Desktop\data";  //Location of data folder
var clicksFolder = new DirectoryInfo($"{dataFolder}\\clicks").GetFiles();
var usersFolder = new DirectoryInfo($"{dataFolder}\\users").GetFiles();
string country = null; // Country filter

Dictionary<string, int> dailyClicks = new();
Dictionary<string, string> idToCountry = new();

if (country != null)
{
    Parallel.ForEach(usersFolder, user =>
    {
        using var usersReader = new StreamReader(user.FullName);
        int i = 0;
        AssignCollumnNumber(usersReader.ReadLine().Split(','), i, out Dictionary<string, int> dataCollumn);

        while (usersReader.EndOfStream == false)
        {
            string[] parts = usersReader.ReadLine().Split(',');
            var requiredPart = parts[dataCollumn["id"]];
            lock (idToCountry) //Locking dictionary so that parallel tasks wouldn't check/append dictionary at he same time
            {
                if (idToCountry.ContainsKey(requiredPart))
                {
                    continue;
                }
                else
                {
                    idToCountry[requiredPart] = parts[dataCollumn["country"]];
                }
            }

        }
    });
}

Parallel.ForEach(clicksFolder, clickInfoFile =>
{
    UpdateClickCount(clickInfoFile);
});

void UpdateClickCount(FileInfo clickInfoFile)
{
    using (var reader = new StreamReader(clickInfoFile.FullName))
    {
        int i = 0;
        AssignCollumnNumber(reader.ReadLine().Split(','), i, out Dictionary<string, int> dataCollumn);

        while (reader.EndOfStream == false)
        {
            string[] parts = reader.ReadLine().Split(',');
            var requiredPart = parts[dataCollumn["date"]];
            var userId = parts[dataCollumn["user_id"]];
            lock (dailyClicks)
            {
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
                            // If there are no clicks from the specified country that day it should still show up in the list with the count 0
                            dailyClicks[requiredPart] = 0;
                        }
                    }
                }
            }

        }
    }
}

WriteToCsv(string.IsNullOrEmpty(country), dailyClicks, dataFolder);

//Assigning correct collumn number as received data is not always in the same format
void AssignCollumnNumber(string[] collumn, int i, out Dictionary<string, int> dataCollumn)
{
    dataCollumn = new Dictionary<string, int>();
    foreach (var item in collumn)
    {
        dataCollumn[item] = i;
        i++;
    }
}

void WriteToCsv(bool hasFilter, Dictionary<string, int> dailyClicks, string dataFolder)
{
    var path = hasFilter ? "total_clicks" : "filtered_clicks";

    DataTable dt = new DataTable { TableName = path };
    dt.Columns.Add("date");
    dt.Columns.Add("count");
    StringBuilder sb = new StringBuilder();

    string[] columnNames = dt.Columns.Cast<DataColumn>().Select(column => column.ColumnName).ToArray();
    sb.AppendLine(string.Join(",", columnNames));

    foreach (var pair in dailyClicks)
    {
        var row = dt.NewRow();
        row["date"] = pair.Key;
        row["count"] = pair.Value;
        string?[] fields = row.ItemArray.Select(field => field.ToString()).ToArray();
        sb.AppendLine(string.Join(",", fields));
    }

    File.WriteAllText($"{dataFolder}\\{path}.csv", sb.ToString());
}
