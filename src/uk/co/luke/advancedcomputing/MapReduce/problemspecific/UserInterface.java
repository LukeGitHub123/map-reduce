package uk.co.luke.advancedcomputing.MapReduce.problemspecific;

import javafx.application.Application;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * The UserInterface class is a JavaFX application which acts as a front end for the application.
 */
public class UserInterface extends Application {
	@Override
	public void start(Stage primaryStage) throws Exception {
		final Button btnSelectPassengersDataFile = new Button("Select Passenger Data File");
		final TextField txtPassengerDataFile = new TextField("");
		txtPassengerDataFile.setEditable(false);
		txtPassengerDataFile.setPrefWidth(300);
		final Button btnSelectAirportsFile = new Button("Select Airports File");
		final TextField txtAirportsFile = new TextField("");
		txtAirportsFile.setPrefWidth(300);
		txtAirportsFile.setEditable(false);
		final Button btnStartProcess = new Button("Start");
		
		final HBox topBar = new HBox();
		topBar.setPadding(new Insets(10,20,20,10));
		topBar.getChildren().addAll(btnSelectPassengersDataFile, txtPassengerDataFile, btnSelectAirportsFile,txtAirportsFile,btnStartProcess);
		topBar.setSpacing(100);
		
		final TextArea errorsOutput = new TextArea();
		errorsOutput.setEditable(false);
		final TextArea objective1Output = new TextArea();
		objective1Output.setEditable(false);
		final TextArea objective2And3Output = new TextArea();
		objective2And3Output.setEditable(false);
		
		
		final HBox outputArea = new HBox();
		outputArea.getChildren().setAll(errorsOutput, objective1Output, objective2And3Output);
		outputArea.setPadding(new Insets(10,10,10,10));
		outputArea.setSpacing(10);
		
		final BorderPane rootLayout = new BorderPane();
		rootLayout.setTop(topBar);
		rootLayout.setCenter(outputArea);
		
		Scene scene = new Scene(rootLayout);
		primaryStage.setScene(scene);
		primaryStage.setMaximized(true);
		primaryStage.show();
		
		// event handlers
		btnSelectPassengersDataFile.setOnAction(e -> {
			FileChooser fileChooser = new FileChooser();
			fileChooser.setTitle("Select the flight data file.");
			FileChooser.ExtensionFilter fileExtensions = new FileChooser.ExtensionFilter("CSV (.csv)", "*.csv");
			
			fileChooser.getExtensionFilters().add(fileExtensions);
			File flightDataFile = fileChooser.showOpenDialog(primaryStage);
			
			if(flightDataFile != null){
				if(flightDataFile.isFile()){
					txtPassengerDataFile.setText(flightDataFile.getPath());
				}else {
					Alert alert = new Alert(Alert.AlertType.ERROR);
					alert.setTitle("Error!");
					alert.setContentText("The file you selected is not valid.");
				}
			}
			
		});
		
		btnSelectAirportsFile.setOnAction(e ->{
			FileChooser fileChooser = new FileChooser();
			fileChooser.setTitle("Select the airports file.");
			FileChooser.ExtensionFilter fileExtensions = new FileChooser.ExtensionFilter("CSV (.csv)", "*.csv");
			
			fileChooser.getExtensionFilters().add(fileExtensions);
			File airportsFile = fileChooser.showOpenDialog(primaryStage);
			if (airportsFile != null){
				if(airportsFile.isFile()){
					txtAirportsFile.setText(airportsFile.getPath());
				}else {
					Alert alert = new Alert(Alert.AlertType.ERROR);
					alert.setTitle("Error!");
					alert.setContentText("The file you selected is not valid.");
				}
			}
			
		});
		
		btnStartProcess.setOnAction(e -> {
			File passengerFile = new File(txtPassengerDataFile.getText());
			File airportFile = new File(txtAirportsFile.getText());
			
			Alert alert = new Alert(Alert.AlertType.ERROR);
			alert.setTitle("Invalid Input File");
			if(!passengerFile.isFile()){
				alert.setContentText("Selected passenger file is not a valid file.");
				alert.show();
				return;
			}
			if(!airportFile.isFile()){
				alert.setContentText("Selected airport file is not a valid file.");
				alert.show();
				return;
			}
			
			btnSelectAirportsFile.setDisable(true);
			btnSelectPassengersDataFile.setDisable(true);
			btnStartProcess.setDisable(true);
			
			
			Main.mapReduce(passengerFile,airportFile);
			
			File errorsFile = new File("Errors.txt");
			File objective1File = new File("objective1.txt");
			File objective2And3File = new File("objective2And3.txt");
			
			errorsOutput.setText("");
			objective1Output.setText("");
			objective2And3Output.setText("");
			
			if(errorsFile.isFile()){
				try {
					Scanner input = new Scanner(errorsFile);
					while (input.hasNextLine()){
						errorsOutput.appendText(input.nextLine() + "\n");
					}
					input.close();
				} catch (FileNotFoundException e1) {
					e1.printStackTrace();
				}
				errorsOutput.setScrollTop(0);
			}
			if(objective1File.isFile()){
				try {
					Scanner input  = new Scanner(objective1File);
					while (input.hasNextLine()){
						objective1Output.appendText(input.nextLine() + "\n");
					}
					input.close();
				} catch (FileNotFoundException e1) {
					e1.printStackTrace();
				}
				objective1Output.setScrollTop(0);
			}
			if(objective2And3File.isFile()){
				
				try {
					Scanner input = new Scanner(objective2And3File);
					while (input.hasNextLine()){
						objective2And3Output.appendText(input.nextLine() + "\n");
					}
					input.close();
				} catch (FileNotFoundException e1) {
					e1.printStackTrace();
				}
				objective2And3Output.setScrollTop(0);
			}
			
			btnSelectAirportsFile.setDisable(false);
			btnSelectPassengersDataFile.setDisable(false);
			btnStartProcess.setDisable(false);
			
		});
	}
	
	/**
	 * This is the main entry point to the whole program.
	 * @param args
	 */
	public static void main(String[] args){
		launch(args);
	}
}
