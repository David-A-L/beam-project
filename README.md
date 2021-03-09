# beam-project
A sample project using Apache Beam. Requires Java 8 and Maven

## ChecksumGenerator
The checksum generator is currently the whole of the project. It contains a pipeline capable of reading a directory of files and outputting a manifest containing the file checksums.
It has two arguments:
- *inputDirectory*: the directory containing the files to use as the pipeline input (default *data*)
- *outputPath*: the path to write *manifest.json* to (optional)

## Testing the Project
The project can be tested with the provided sample data. Running the following command in your terminal will compile the project and generate a manifest for the single file contained in *sampleData*:
`$ mvn compile exec:java -D exec.mainClass=com.latva.app.beam.ChecksumGenerator -D exec.args="--inputDirectory=sampledata  --outputPath=sample/" -P direct-runner`

The output in *manifest.json* will be the following:
>{"input-0001":"92abc8dfc8a97fdc8d718b63afb617b603ba70839bbef877499417df1e407f38"}

## Future Improvements
- Refactor and reorganize Fns: Some can be made more generic and reusable, the project should be better structured to scale with more added functionality
- Unit tests: Originally wanted to get testing set up but wasn't able to for lack of time. The dependencies are there, just need to write the tests
- Pull out configuration options: Checksum Generator has some hardcoded values such as the memory buffer size and the algorithm used to generate the checksums
- Better error handling: will want to better handle failures and permission issues when handling files