package uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures;

/**
 * The VerboseInputException class is designed to be a type of input exception which holds the error as well as the line if a file it occurs on for more relevant error handling.
 */
public class VerboseInputException extends InputException {
	
	String fileLine;
	String fileType;
	
	public VerboseInputException(InputException inputException, String lineFromFile, String fileType){
		super(inputException.getInputName(), inputException.getReasons());
		setFileLine(lineFromFile);
		setFileType(fileType);
	}
	
	public String getFileLine() {
		return fileLine;
	}
	
	public void setFileLine(String fileLine) {
		this.fileLine = fileLine;
	}
	@Override
	public String getErrorAsJsonString(){
		String inputException = super.getErrorAsJsonString();
		String jsonString = "{\n" +
				                    "\tFileType: '" + this.fileType + "',\n" +
									"\tFileLine: '" + this.fileLine + "',\n" +
									"\tException: " + inputException + "\n}";
		
		return jsonString;
	}
	
	public String getFileType() {
		return fileType;
	}
	
	public void setFileType(String fileType) {
		this.fileType = fileType;
	}
}
