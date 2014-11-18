package driver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.IOUtils;
import org.cloudbus.cloudsim.Log;

import cws.core.algorithms.Algorithm;
import cws.core.algorithms.AlgorithmStatistics;
import cws.core.algorithms.DPDS;
import cws.core.algorithms.SPSS;
import cws.core.algorithms.WADPDS;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.config.GlobalStorageParamsLoader;
import cws.core.core.VMType;
import cws.core.core.VMTypeLoader;
import cws.core.dag.DAG;
import cws.core.dag.DAGListGenerator;
import cws.core.dag.DAGParser;
import cws.core.dag.DAGStats;
import cws.core.dag.Task;
import cws.core.engine.Environment;
import cws.core.engine.EnvironmentFactory;
import cws.core.exception.IllegalCWSArgumentException;
import cws.core.VMFactory;
import cws.core.storage.StorageManagerStatistics;
import cws.core.storage.global.GlobalStorageParams;

import cws.core.simulation.StorageSimulationParams;
import cws.core.simulation.StorageType;
import cws.core.simulation.StorageCacheType;



public class MySimulation {

    public String[] distributionFactory(String inputName, String distribution,
                                        int ensembleSize, long seed) {
        if ("uniform_unsorted".equals(distribution)) {
            return DAGListGenerator.generateDAGListUniformUnsorted(new Random(seed), inputName, ensembleSize);
        } else if ("uniform_sorted".equals(distribution)) {
            return DAGListGenerator.generateDAGListUniform(new Random(seed), inputName, ensembleSize);
        } else if ("pareto_unsorted".equals(distribution)) {
            return DAGListGenerator.generateDAGListParetoUnsorted(new Random(seed), inputName, ensembleSize);
        } else if ("pareto_sorted".equals(distribution)) {
            return DAGListGenerator.generateDAGListPareto(new Random(seed), inputName, ensembleSize);
        } else if ("constant".equals(distribution)) {
            return DAGListGenerator.generateDAGListConstant(new Random(seed), inputName, ensembleSize);
        } else if (distribution.startsWith("fixed")) {
            int size = Integer.parseInt(distribution.substring(5));
            return DAGListGenerator.generateDAGListConstant(inputName, size, ensembleSize);
        } else {
            throw new IllegalCWSArgumentException("Unrecognized distribution: " + distribution);
        }
    }

    /**
     * The scaling factor for jobs' runtimes.
     */
    private static final String DEFAULT_SCALING_FACTOR = "1.0";

    /**
     * Whether to enable simulation logging. It is needed for validation and gantt graphs generation, but can decrease
     * performance especially if logs are dumped to stdout.
     */
    private static final String DEFAULT_ENABLE_LOGGING = "true";

    /**
     * Number of budgets generated. It is ignored when budget is explicitly set.
     */
    private static final String DEFAULT_N_BUDGETS = "10";

    /**
     * Number of deadlines generated. It is ignored when deadline is explicitly set.
     */
    private static final String DEFAULT_N_DEADLINES = "10";

    /**
     * How many times more can the number of VMs be increased? 1.0 means 0%, 2.0 means 100%, etc..
     */
    private static final String DEFAULT_MAX_SCALING = "1.0";

    /**
     * The algorithm alpha parameter.
     */
    private static final String DEFAULT_ALPHA = "0.7";

    /**
     * Loads VMType from file and/or from CLI args
     */
    private final VMTypeLoader vmTypeLoader;

    public MySimulation(VMTypeLoader vmTypeLoader) {
        this.vmTypeLoader = vmTypeLoader;
    }

    public static Options buildOptions() {
        Options options = new Options();

        Option seed = new Option("s", "seed", true, "Random number generator seed, defaults to current time in milis");
        seed.setArgName("SEED");
        options.addOption(seed);

        Option application = new Option("app", "application", true, "(required) Application name");
        application.setRequired(true);
        application.setArgName("APP");
        options.addOption(application);

        Option inputdir = new Option("id", "input-dir", true, "(required) Input dir");
        inputdir.setRequired(true);
        inputdir.setArgName("DIR");
        options.addOption(inputdir);

        Option outputfile = new Option("of", "output-file", true, "(required) Output file");
        outputfile.setRequired(true);
        outputfile.setArgName("FILE");
        options.addOption(outputfile);

        Option distribution = new Option("dst", "distribution", true, "(required) Distribution");
        distribution.setRequired(true);
        distribution.setArgName("DIST");
        options.addOption(distribution);

        Option algorithm = new Option("alg", "algorithm", true, "(required) Algorithm");
        algorithm.setRequired(true);
        algorithm.setArgName("ALGO");
        options.addOption(algorithm);

        Option scalingFactor = new Option("sf", "scaling-factor", true, "Scaling factor, defaults to "
                                          + DEFAULT_SCALING_FACTOR);
        scalingFactor.setArgName("FACTOR");
        options.addOption(scalingFactor);

        Option enableLogging = new Option("el", "enable-logging", true, "Whether to enable logging, defaults to "
                                          + DEFAULT_ENABLE_LOGGING);
        enableLogging.setArgName("BOOL");
        options.addOption(enableLogging);

        Option deadline = new Option("d", "deadline", true, "Optional deadline, which overrides max and min deadlines");
        deadline.setArgName("DEADLINE");
        options.addOption(deadline);

        Option budget = new Option("b", "budget", true, "Optional budget, which overrides max and min budgets");
        budget.setArgName("BUDGET");
        options.addOption(budget);

        Option nBudgets = new Option("nb", "n-budgets", true, "Optional number of generated budgets, defaults to "
                                     + DEFAULT_N_BUDGETS);
        nBudgets.setArgName("N");
        options.addOption(nBudgets);

        Option nDeadlines = new Option("nd", "n-deadlines", true,
                                       "Optional number of generated deadlines, defaults to " + DEFAULT_N_DEADLINES);
        nDeadlines.setArgName("N");
        options.addOption(nDeadlines);

        Option maxScaling = new Option("ms", "max-scaling", true,
                                       "Optional maximum VM number scaling factor, defaults to " + DEFAULT_MAX_SCALING);
        maxScaling.setArgName("FLOAT");
        options.addOption(maxScaling);

        Option alpha = new Option("alp", "alpha", true, "Optional alpha factor, defaults to " + DEFAULT_ALPHA);
        alpha.setArgName("FLOAT");
        options.addOption(alpha);

        VMFactory.buildCliOptions(options);

        VMTypeLoader.buildCliOptions(options);
        GlobalStorageParamsLoader.buildCliOptions(options);

        return options;
    }

    private static void printUsage(Options options, String reason) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(120);
        formatter.printHelp(MySimulation.class.getName(), "", options, reason);
        System.exit(1);
    }

    public static void main(String[] args) {
        Options options = buildOptions();
        CommandLine cmd = null;
        try {
            CommandLineParser parser = new PosixParser();
            cmd = parser.parse(options, args);
        } catch (ParseException exp) {
            printUsage(options, exp.getMessage());
        }
        MySimulation testRun = new MySimulation(new VMTypeLoader());
        try {
            testRun.runTest(cmd);
        } catch (IllegalCWSArgumentException e) {
            printUsage(options, e.getMessage());
        }
    }

    public void runTest(CommandLine args) {
        // Arguments with no defaults
        String algorithmName = args.getOptionValue("algorithm");
        String application = args.getOptionValue("application");
        File inputdir = new File(args.getOptionValue("input-dir"));
        File outputfile = new File(args.getOptionValue("output-file"));
        String distribution = args.getOptionValue("distribution");
        String storageManagerType = args.getOptionValue("storage-manager");

        // Arguments with defaults
        int ensembleSize = 1;
        double scalingFactor = Double.parseDouble(args.getOptionValue("scaling-factor", DEFAULT_SCALING_FACTOR));
        long seed = Long.parseLong(args.getOptionValue("seed", System.currentTimeMillis() + ""));
        boolean enableLogging = Boolean.valueOf(args.getOptionValue("enable-logging", DEFAULT_ENABLE_LOGGING));
        int nbudgets = Integer.parseInt(args.getOptionValue("n-budgets", DEFAULT_N_BUDGETS));
        int ndeadlines = Integer.parseInt(args.getOptionValue("n-deadlines", DEFAULT_N_DEADLINES));
        double maxScaling = Double.parseDouble(args.getOptionValue("max-scaling", DEFAULT_MAX_SCALING));
        double alpha = Double.parseDouble(args.getOptionValue("max-scaling", DEFAULT_ALPHA));

        VMType vmType = vmTypeLoader.determineVMType(args);

        VMFactory.readCliOptions(args, seed);



        double budget = Double.valueOf(args.getOptionValue("budget", "1e10"));
        double deadline = Double.valueOf(args.getOptionValue("deadline", "1e10"));


        // Make CloudSim object
        CloudSimWrapper cloudsim = null;
        if (enableLogging) {
            cloudsim = new CloudSimWrapper(getLogOutputStream(budget, deadline, outputfile));
        } else {
            cloudsim = new CloudSimWrapper();
        }
        cloudsim.init();
        cloudsim.setLogsEnabled(enableLogging);


        // We do not need Cloudsim's logs. We have our own.
        Log.disable();

        // Determine the distribution
        String inputName = inputdir.getAbsolutePath() + "/" + application;
        String[] distributionNames = distributionFactory(inputName, distribution,
                                                         ensembleSize, seed);

        // Use trivial storage simulation only
        StorageSimulationParams simulationParams = new StorageSimulationParams();
        simulationParams.setStorageCacheType(StorageCacheType.VOID);
        simulationParams.setStorageType(StorageType.VOID);


        // Make the environment
        Environment environment = EnvironmentFactory.createEnvironment
            (cloudsim, simulationParams, vmType);

        // Parse the dags
        List<DAG> dags = parseDags(distributionNames, scalingFactor);

        // Make the algorithm
        Algorithm algorithm = createAlgorithm(alpha, maxScaling, algorithmName,
                                              cloudsim, dags, budget,
                                              deadline, environment);


        // Run
        // ============================================================
        algorithm.simulate();


        // Generate stats about how well the job did
        // ============================================================

        AlgorithmStatistics algorithmStatistics = algorithm.getAlgorithmStatistics();
        double planningTime = algorithm.getPlanningnWallTime() / 1.0e9;
        double simulationTime = cloudsim.getSimulationWallTime() / 1.0e9;
        StorageManagerStatistics stats = environment.getStorageManagerStatistics();
    }

    private void logWorkflowsDescription(List<DAG> dags, String[] distributionNames,
                                         CloudSimWrapper cloudsim) {
        for (int i = 0; i < dags.size(); i++) {
            DAG dag = dags.get(i);

            String workflowDescription =
                String.format("Workflow %s, priority = %d, filename = %s",
                              dag.getId(),
                              dags.size() - i,
                              distributionNames[i]);

                cloudsim.log(workflowDescription);
            }
        }

        /**
         * Crates algorithm instance from the given input params.
         * @param environment
         * @return The newly created algorithm instance.
         */
        protected Algorithm createAlgorithm(double alpha, double maxScaling, String algorithmName,
                                            CloudSimWrapper cloudsim, List<DAG> dags, double budget, double deadline, Environment environment) {
            AlgorithmStatistics ensembleStatistics = new AlgorithmStatistics(dags, budget, deadline, cloudsim);

            if ("SPSS".equals(algorithmName)) {
                return new SPSS(budget, deadline, dags, alpha, ensembleStatistics, environment, cloudsim);
            } else {
                throw new IllegalCWSArgumentException("Unknown algorithm: " + algorithmName);
            }
        }

        /**
         * Returns output stream for logs for current simulation.
         * @param budget The simulation's budget.
         * @param deadline The simulation's deadline.
         * @param outputfile The simulation's main output file.
         * @return Output stream for logs for current simulation.
         */
        private OutputStream getLogOutputStream(double budget, double deadline,
                                                File outputfile) {
            String name = String.format("%s.b-%.2f-d-%.2f.log",
                                        outputfile.getAbsolutePath(),
                                        budget, deadline);
            try {
                return new FileOutputStream(new File(name));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        // Load dags from files
        private List<DAG> parseDags(String[] distributionNames, double scalingFactor)
        {
            List<DAG> dags = new ArrayList<DAG>();
            int workflow_id = 0;

            for (String name : distributionNames) {
                DAG dag = DAGParser.parseDAG(new File(name));
                dag.setId(new Integer(workflow_id).toString());

                System.out.println(String.format("Workflow %d, priority = %d, filename = %s",
                                                 workflow_id,
                                                 distributionNames.length - workflow_id,
                                                 name));

                workflow_id++;
                dags.add(dag);

                if (scalingFactor > 1.0) {
                    for (String tid : dag.getTasks()) {
                        Task t = dag.getTaskById(tid);
                        t.scaleSize(scalingFactor);
                    }
                }
            }
            return dags;
        }

    }
