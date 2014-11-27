package driver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import java.util.Set;
import java.util.HashSet;



import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.IOUtils;
import org.cloudbus.cloudsim.Log;

import cws.core.Cloud;
import cws.core.VM;
import cws.core.EnsembleManager;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;


import cws.core.algorithms.Algorithm;
import cws.core.algorithms.AlgorithmStatistics;
import cws.core.algorithms.DPDS;
import cws.core.algorithms.SPSS;
import cws.core.algorithms.WADPDS;
import cws.core.algorithms.DynamicAlgorithm;
import cws.core.algorithms.heterogeneous.StaticHeterogeneousAlgorithm;
import cws.core.algorithms.heterogeneous.Planner;
import cws.core.algorithms.heterogeneous.TrivialPlanner;




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

import cws.core.Scheduler;
import cws.core.scheduler.DAGSchedulerFCFS;
import cws.core.Provisioner;
import cws.core.provisioner.NullProvisioner;
import cws.core.provisioner.CloudAwareProvisioner;
import cws.core.provisioner.SimpleUtilizationBasedProvisioner;



import cws.core.storage.StorageManagerStatistics;
import cws.core.simulation.StorageSimulationParams;
import cws.core.simulation.StorageType;
import cws.core.simulation.StorageCacheType;



public class MySimulation {

    /**
     * Loads VMType from file and/or from CLI args
     */
    private final VMTypeLoader vmTypeLoader;

    public MySimulation(VMTypeLoader vmTypeLoader) {
        this.vmTypeLoader = vmTypeLoader;
    }

    public static Options buildOptions() {
        Options options = new Options();

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

        Option deadline = new Option("d", "deadline", true, "Optional deadline, which overrides max and min deadlines");
        deadline.setArgName("DEADLINE");
        options.addOption(deadline);

        Option budget = new Option("b", "budget", true, "Optional budget, which overrides max and min budgets");
        budget.setArgName("BUDGET");
        options.addOption(budget);

        VMFactory.buildCliOptions(options);

        VMTypeLoader.buildCliOptions(options);

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

        // Parse arguments
        String application = args.getOptionValue("application");
        File inputdir = new File(args.getOptionValue("input-dir"));
        File outputfile = new File(args.getOptionValue("output-file"));
        double budget = Double.valueOf(args.getOptionValue("budget", "100000"));
        double deadline = Double.valueOf(args.getOptionValue("deadline", "100000"));

        // Make VMType and VMFactory objects
        VMType vmType = vmTypeLoader.determineVMType(args);
        VMFactory.readCliOptions(args, System.currentTimeMillis());

        // Make CloudSim object
        OutputStream logStream = getLogOutputStream(budget, deadline, outputfile);
        CloudSimWrapper cloudsim = new CloudSimWrapper(logStream);
        cloudsim.init();
        cloudsim.setLogsEnabled(true);

        // We do not need Cloudsim's logs. We have our own.
        Log.disable();

        // Determine the distribution
        String inputName = inputdir.getAbsolutePath() + "/" + application;
        Random rand = new Random(System.currentTimeMillis());
        String[] distributionNames =
            DAGListGenerator.generateDAGListConstant(inputName, 50, 1);

        // Use trivial storage simulation only
        StorageSimulationParams simulationParams = new StorageSimulationParams();
        simulationParams.setStorageCacheType(StorageCacheType.VOID);
        simulationParams.setStorageType(StorageType.VOID);

        // Make the environment
        Environment environment = EnvironmentFactory.createEnvironment
            (cloudsim, simulationParams, vmType);

        // Parse the dags
        List<DAG> dags = parseDags(distributionNames, 1.0);

        // Initial logs
        cloudsim.log("budget = " + budget);
        cloudsim.log("deadline = " + deadline);
        logWorkflowsDescription(dags, distributionNames, cloudsim);

        List<VMType> vmtypes = new ArrayList<VMType>();
        vmtypes.add(vmType);

        // Build our cloud
        Cloud cloud = new Cloud(cloudsim);

        // Make the algorithm
        AlgorithmStatistics ensembleStatistics =
            new AlgorithmStatistics(dags, budget, deadline, cloudsim);

        Provisioner provisioner = new NullProvisioner();
        Planner planner = new TrivialPlanner();

        StaticHeterogeneousAlgorithm staticAlgo = new StaticHeterogeneousAlgorithm
            (budget, deadline, dags, ensembleStatistics,
             planner, vmtypes, cloudsim);
        Algorithm algorithm = staticAlgo;
        Scheduler scheduler = staticAlgo;

        WorkflowEngine engine = new WorkflowEngine(provisioner, scheduler,
                                                   budget, deadline, cloudsim);
        EnsembleManager manager = new EnsembleManager(engine, cloudsim);

        algorithm.setWorkflowEngine(engine);
        algorithm.setCloud(cloud);
        algorithm.setEnsembleManager(manager);
        // cloud.addVMListener(algorithm);
        // engine.addJobListener(algorithm);


        // // launch VMs
        // for(VM vm : vms) {
        //     cloudsim.send(engine.getId(), cloud.getId(), 0.0,
        //                   WorkflowEvent.VM_LAUNCH, vm);
        // }

        // // kill VMs before deadline
        // for(VM vm : vms) {
        //     double delay = vm.getVmType().getDeprovisioningDelayEstimation()*2;
        //     cloudsim.send(engine.getId(), cloud.getId(), deadline - delay,
        //                   WorkflowEvent.VM_TERMINATE, vm);
        // }



        // Run
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
