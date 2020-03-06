package com.scriptexecutor;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    public static void main(final String[] args) {
        LOGGER.log(Level.INFO, "Starting...");
        ScriptExecutor.INSTANCE.run();
        LOGGER.log(Level.INFO, "... Finished");
    }
}
