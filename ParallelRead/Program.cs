namespace ParallelRead
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    using System.IO;
    using System.Linq;
    using System.Text;

    public class Program
    {
        public static void Main(string[] args)
        {
            var fileName = @"C:\Users\callum.linington\Documents\Visual Studio 2015\TrashProjects\ElasticSearchPopulator\src\ElasticSearchPopulator.Gui\bin\Debug\citizens.csv";

            var readAFile = new ReadAFile();

            var taskResult = readAFile.OpenReadFileAsync(fileName, 10);
            var result = taskResult.Result;

            if (taskResult.IsFaulted)
            {
                Console.WriteLine(taskResult.Exception?.Message);
                if (taskResult.Exception?.InnerException != null)
                {
                    Console.WriteLine(taskResult.Exception.InnerException.Message);
                }
            }

            var positions = readAFile.GetReadPositions(result, fileName).Result;

            positions.ForEach(x => Console.WriteLine($"Reading: {x.Start} - {x.End}"));

            var concurrentReads = new List<Task<List<string>>>();

            positions.ForEach(x => concurrentReads.Add(readAFile.ReadConcurrently(fileName, x.Start, x.End)));

            var allResults = Task.WhenAll(concurrentReads).Result;

            var concattedListOfStrings = allResults.SelectMany(x => x);

            Console.WriteLine($"Results: {concattedListOfStrings.Count()}");

            Console.WriteLine($"Empty Rows: {concattedListOfStrings.Count(string.IsNullOrWhiteSpace)}");

            Console.ReadKey();
        }
    }

    public class ReadAFile
    {
        private const int BufferSize = 4096;

        public async Task<List<long>> OpenReadFileAsync(string fileName, int concurrentReads)
        {

            var buffer = new char[BufferSize];

            var endpoints = new List<long>();

            using (var fileStream = this.CreateMultipleReadAccessFileStream(fileName))
            {
                var fileLength = fileStream.Length;

                var seekPositionCount = fileLength / concurrentReads;

                long currentOffset = 0;
                for (var i = 0; i < concurrentReads; i++)
                {
                    var seekPosition = seekPositionCount + currentOffset;

                    // seek the file forward
                    // fileStream.Seek(seekPosition, SeekOrigin.Current);

                    // setting true at the end is very important, keeps the underlying fileStream open.
                    using (var streamReader = this.CreateTemporaryStreamReader(fileStream))
                    {
                        // this is poor on performance, hence why you split the file here and read in new threads.
                        streamReader.DiscardBufferedData();
                        // you have to advance the fileStream here, because of the previous line
                        streamReader.BaseStream.Seek(seekPosition, SeekOrigin.Begin);
                        // this also seeks the file forward the amount in the buffer...
                        int bytesRead;
                        var totalBytesRead = 0;
                        while ((bytesRead = await streamReader.ReadAsync(buffer, 0, buffer.Length)) > 0)
                        {
                            totalBytesRead += bytesRead;

                            var found = false;

                            var gotR = false;

                            for (var j = 0; j < buffer.Length; j++)
                            {
                                if (buffer[j] == '\r')
                                {
                                    gotR = true;
                                    continue;
                                }

                                if (buffer[j] == '\n' && gotR)
                                {
                                    // so we add the total bytes read, minus the current buffer amount read, then add how far into the buffer we actually read.
                                    seekPosition += totalBytesRead - BufferSize + j;
                                    endpoints.Add(seekPosition);
                                    found = true;
                                    break;
                                }
                                // if we have found new line then move the position to 
                            }

                            if (found) break;
                        }
                    }

                    currentOffset = seekPosition;
                }
            }

            return endpoints;
        }

        public async Task<TriState> IsCorrectPositionAsync(string fileName, long endPos)
        {
            var buffer = new char[BufferSize];

            using (var fileStream = this.CreateMultipleReadAccessFileStream(fileName))
            using(var streamReader = this.CreateTemporaryStreamReader(fileStream))
            {
                streamReader.DiscardBufferedData();
                streamReader.BaseStream.Seek(endPos - 1, SeekOrigin.Begin);

                var readBytes = await streamReader.ReadAsync(buffer, 0, BufferSize);

                if (readBytes < 1)
                {
                    return TriState.False();
                }

                if (buffer[0] == '\n')
                {
                    return TriState.SortOf();
                }

                if (buffer[1] == '\n')
                {
                    return TriState.True();
                }
            }

            return TriState.False();
        }

        public async Task<List<string>> ReadConcurrently(string fileName, long startPos, long? endPos)
        {
            // If endPos is Null, then read to the end of the file.

            var buffer = new char[BufferSize];

            char[] finalBuffer = null;

            if (endPos.HasValue)
            {
                finalBuffer = new char[endPos.Value - startPos];
            }

            using (var fileStream = this.CreateMultipleReadAccessFileStream(fileName))
            using (var streamReader = this.CreateTemporaryStreamReader(fileStream))
            {
                if (!endPos.HasValue)
                {
                    finalBuffer = new char[fileStream.Length-startPos];
                }

                streamReader.DiscardBufferedData();
                streamReader.BaseStream.Seek(startPos, SeekOrigin.Begin);

                var finalPos = 0;
                var done = false;

                while (await streamReader.ReadAsync(buffer, 0, BufferSize) > 0)
                {
                    foreach (var c in buffer)
                    {
                        if (finalPos == finalBuffer.Length)
                        {
                            done = true;
                            break;
                        }
                        finalBuffer[finalPos] = c;
                        finalPos++;
                    }

                    if (done)
                    {
                        break;
                    }
                }
            }

            var finalResult = new string(finalBuffer);

            return finalResult
                        .Split(new []{Environment.NewLine}, StringSplitOptions.None)
                        .ToList();
        }

        public async Task<List<ReadPosition>> GetReadPositions(List<long> endpoints, string fileName)
        {
            var positions = new List<ReadPosition>();

            long startingPos = 0;

            foreach (var x in endpoints)
            {
                // var triState = await this.IsCorrectPositionAsync(fileName, x);

                positions.Add(new ReadPosition(startingPos, x));

                startingPos = x + 1;
            }

            positions.Add(new ReadPosition(startingPos));

            return positions;
        }

        private StreamReader CreateTemporaryStreamReader(FileStream fileStream)
        {
            return new StreamReader(fileStream, Encoding.UTF8, true, BufferSize, true);
        }

        private FileStream CreateMultipleReadAccessFileStream(string fileName)
        {
            return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read);
        }
    }

    public class ReadPosition
    {
        public long Start { get; }

        public long? End { get; }

        public ReadPosition(long start)
            : this(start, null)
        {
        }

        public ReadPosition(long start, long? end)
        {
            this.Start = start;
            this.End = end;
        }
    }

    public class TriState
    {
        public string State { get; }

        internal TriState(string state)
        {
            this.State = state;
        }

        public static TriState True()
        {
            return new TriState("true");
        }

        public static TriState SortOf()
        {
            return new TriState("sort of true");
        }

        public static TriState False()
        {
            return new TriState("false");
        }

        public override string ToString()
        {
            return this.State;
        }
    }
}
