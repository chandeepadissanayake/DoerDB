package com.doerit.doerdb.cli;

import com.doerit.doerdb.DBCredentialWrapper;
import com.doerit.doerdb.exceptions.InvalidException;
import com.doerit.doerdb.util.DatabaseConverter;
import org.apache.commons.cli.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CLIProcessor {

    private final CommandLine cliArgs;

    public CLIProcessor(String[] args) throws ParseException {
        CLIOptions wrapperOptions = new CLIOptions();
        Options cliOptions = wrapperOptions.getCliOptions();

        CommandLineParser cliParser = new DefaultParser();
        this.cliArgs = cliParser.parse(cliOptions, args);
    }

    public void processArgs() {
        if (this.cliArgs.hasOption(CLIOptions.NAME_OPTION_CONVERT_DB)) {
            this.processConvertDB();
        }
    }

    private void processConvertDB() {
        List<String> shouldHaveOptionsNames = new ArrayList<String>() {{
            add(CLIOptions.NAME_OPTION_LOCAL_DB_HOST);
            add(CLIOptions.NAME_OPTION_LOCAL_DB_PORT);
            add(CLIOptions.NAME_OPTION_LOCAL_DB_DB_NAME);
            add(CLIOptions.NAME_OPTION_LOCAL_DB_USERNAME);
            add(CLIOptions.NAME_OPTION_LOCAL_DB_PASSWORD);

            add(CLIOptions.NAME_OPTION_REMOTE_DB_HOST);
            add(CLIOptions.NAME_OPTION_REMOTE_DB_PORT);
            add(CLIOptions.NAME_OPTION_REMOTE_DB_DB_NAME);
            add(CLIOptions.NAME_OPTION_REMOTE_DB_USERNAME);
            add(CLIOptions.NAME_OPTION_REMOTE_DB_PASSWORD);
        }};

        for (String shouldHaveOptionName : shouldHaveOptionsNames) {
            if (!this.cliArgs.hasOption(shouldHaveOptionName)) {
                System.err.println("Required Argument " + shouldHaveOptionName + " missing.");
                return;
            }
        }

        try {
            DBCredentialWrapper localDBCredentials = new DBCredentialWrapper(
                    this.cliArgs.getOptionValue(CLIOptions.NAME_OPTION_LOCAL_DB_HOST),
                    Integer.parseInt(this.cliArgs.getOptionValue(CLIOptions.NAME_OPTION_LOCAL_DB_PORT)),
                    this.cliArgs.getOptionValue(CLIOptions.NAME_OPTION_LOCAL_DB_DB_NAME),
                    this.cliArgs.getOptionValue(CLIOptions.NAME_OPTION_LOCAL_DB_USERNAME),
                    this.cliArgs.getOptionValue(CLIOptions.NAME_OPTION_LOCAL_DB_PASSWORD)
            );
            DBCredentialWrapper remoteDBCredentials = new DBCredentialWrapper(
                    this.cliArgs.getOptionValue(CLIOptions.NAME_OPTION_REMOTE_DB_HOST),
                    Integer.parseInt(this.cliArgs.getOptionValue(CLIOptions.NAME_OPTION_REMOTE_DB_PORT)),
                    this.cliArgs.getOptionValue(CLIOptions.NAME_OPTION_REMOTE_DB_DB_NAME),
                    this.cliArgs.getOptionValue(CLIOptions.NAME_OPTION_REMOTE_DB_USERNAME),
                    this.cliArgs.getOptionValue(CLIOptions.NAME_OPTION_REMOTE_DB_PASSWORD)
            );

            DatabaseConverter dbConverter = new DatabaseConverter(localDBCredentials, remoteDBCredentials);
            dbConverter.convertToDoerDB();

            System.out.println("Successfully Converted.");
        }

        catch (NumberFormatException numFormatEx) {
            System.err.println("Invalid Port Numbers. Please recheck your local and remote port numbers.");
        }

        catch (SQLException sqlEx) {
            System.err.println("Database failure.\nError Message: " + sqlEx.getMessage());
        }

        catch (InvalidException invalidEx) {
            System.err.println(invalidEx.getMessage());
        }
    }

}
