using System.Data;
using System.Text;

string dataFolder = @"C:\Users\ave\Desktop\data";  //Location of data folder 
string country = null; // Country filter

FileInfo[] clicksFolder = new DirectoryInfo($"{dataFolder}\\clicks").GetFiles();
FileInfo[] usersFolder = new DirectoryInfo($"{dataFolder}\\users").GetFiles();
Dictionary<string, int> dailyClicks = new();
Dictionary<string, string> idToCountry = new();

if (country != null)
{
    Parallel.ForEach(usersFolder, user =>
    {
        using StreamReader usersReader = new(user.FullName);
        int i = 0;
        AssignCollumnNumber(usersReader.ReadLine().Split(','), i, out Dictionary<string, int> dataCollumn);

        while (usersReader.EndOfStream == false)
        {
            string[] parts = usersReader.ReadLine().Split(',');
            string requiredPart = parts[dataCollumn["id"]];
            lock (idToCountry) //Locking dictionary so that parallel tasks wouldn't check/append the dictionary at the same time
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
    using StreamReader reader = new(clickInfoFile.FullName);
    int i = 0;
    AssignCollumnNumber(reader.ReadLine().Split(','), i, out Dictionary<string, int> dataCollumn);

    while (reader.EndOfStream == false)
    {
        string[] parts = reader.ReadLine().Split(',');
        string requiredPart = parts[dataCollumn["date"]];
        string userId = parts[dataCollumn["user_id"]];
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
});

WriteToCsv(!string.IsNullOrEmpty(country), dailyClicks, dataFolder);

//Assigning correct collumn number as received data is not always in the same format
void AssignCollumnNumber(string[] collumn, int i, out Dictionary<string, int> dataCollumn)
{
    dataCollumn = new Dictionary<string, int>();
    foreach (string item in collumn)
    {
        dataCollumn[item] = i;
        i++;
    }
}

void WriteToCsv(bool hasFilter, Dictionary<string, int> dailyClicks, string dataFolder)
{
    string fileName = hasFilter ? "filtered_clicks" : "total_clicks";

    DataTable dt = new() { TableName = fileName };
    dt.Columns.Add("date");
    dt.Columns.Add("count");
    StringBuilder sb = new();

    string[] columnNames = dt.Columns.Cast<DataColumn>().Select(column => column.ColumnName).ToArray();
    sb.AppendLine(string.Join(",", columnNames));

    foreach (KeyValuePair<string, int> pair in dailyClicks)
    {
        DataRow row = dt.NewRow();
        row["date"] = pair.Key;
        row["count"] = pair.Value;
        string?[] fields = row.ItemArray.Select(field => field.ToString()).ToArray();
        sb.AppendLine(string.Join(",", fields));
    }

    File.WriteAllText($"{dataFolder}\\{fileName}.csv", sb.ToString());
}
