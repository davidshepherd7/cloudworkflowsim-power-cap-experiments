package driver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.FileInputStream;
import java.io.InputStream;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import java.util.SortedSet;
import java.util.TreeMap;

import com.lexicalscope.jewel.cli.Option;
import com.lexicalscope.jewel.cli.ArgumentValidationException;
import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.HelpRequestedException;

import org.yaml.snakeyaml.Yaml;

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
import cws.core.algorithms.heterogeneous.PowerCappedPlanner;
import cws.core.algorithms.heterogeneous.TrivialPlanner;
import cws.core.algorithms.heterogeneous.HeftPlanner;
import cws.core.algorithms.heterogeneous.PiecewiseConstantFunction;

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



public final class MySimulation {

    // Non-instantiable
    private MySimulation() {
        throw new AssertionError();
    }

    public static interface Args {
        @Option String getInputDir();

        @Option String getOutputFile();

        @Option String getVmFile();

        @Option(helpRequest = true) boolean getHelp();
    }

    public static void main(String[] commandLine)
            throws ArgumentValidationException, FileNotFoundException {

        // Read arguments
        Args args = null;
        try {
            args = CliFactory.parseArguments(Args.class, commandLine);
        } catch (HelpRequestedException e) {
            System.out.println(e);
            System.exit(0);
        }

        VMType vmType = new VMTypeLoader().determineVMType(args.getVmFile());

        // Run
        // MySimulation testRun = new MySimulation();
        runTest(args.getInputDir(), args.getOutputFile(), vmType);
    }

    public static void runTest(String inputDir, String OutputFile, VMType vmType) {

        // Parse arguments
        String application = "MONTAGE"; // GENOME LIGO SIPHT MONTAGE CYBERSHAKE
        File inputdir = new File(inputDir);
        File outputfile = new File(OutputFile);
        double budget = 100000;
        double deadline = 100000;

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

        List<VMType> vms = Arrays.asList(vmType);

        // Build our cloud
        Cloud cloud = new Cloud(cloudsim);

        PiecewiseConstantFunction powercap = new PiecewiseConstantFunction();
        powercap.addJump(0.0, 101.0); // 2 vms
        powercap.addJump(10.0, 51.0); // 1 vm
        powercap.addJump(20.0, 201.0); // 4 vms

        Provisioner provisioner = new NullProvisioner();
        Planner planner = new PowerCappedPlanner(powercap,
                new HeftPlanner());

        StaticHeterogeneousAlgorithm staticAlgo =
                new StaticHeterogeneousAlgorithm.Builder(dags, planner, cloudsim)
                .budget(budget)
                .deadline(deadline)
                .addInitialVMs(vms)
                .build();
        Algorithm algorithm = staticAlgo;
        Scheduler scheduler = staticAlgo;

        WorkflowEngine engine = new WorkflowEngine(provisioner, scheduler,
                budget, deadline, cloudsim);
        EnsembleManager manager = new EnsembleManager(engine, cloudsim);

        algorithm.setWorkflowEngine(engine);
        algorithm.setCloud(cloud);
        algorithm.setEnsembleManager(manager);

        // Run
        algorithm.simulate();


        // Generate stats about how well the job did
        // ============================================================

        AlgorithmStatistics algorithmStatistics = algorithm.getAlgorithmStatistics();
        double planningTime = algorithm.getPlanningnWallTime() / 1.0e9;
        double simulationTime = cloudsim.getSimulationWallTime() / 1.0e9;
        StorageManagerStatistics stats = environment.getStorageManagerStatistics();

        System.out.println("Power usage was: "
                + algorithmStatistics.getPowerUsage().toString());
    }

    private static void logWorkflowsDescription(List<DAG> dags,
            String[] distributionNames,
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
    private static OutputStream getLogOutputStream(double budget, double deadline,
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
    private static List<DAG> parseDags(String[] distributionNames, double scalingFactor)
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
