import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.Collections;
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
import cws.core.core.PiecewiseConstantFunction;

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
import cws.core.Provisioner;
import cws.core.provisioner.NullProvisioner;
import cws.core.provisioner.CloudAwareProvisioner;
import cws.core.provisioner.SimpleUtilizationBasedProvisioner;

import cws.core.storage.StorageManagerStatistics;
import cws.core.storage.StorageManagerFactory;
import cws.core.storage.StorageManager;
import cws.core.simulation.StorageSimulationParams;
import cws.core.simulation.StorageType;
import cws.core.simulation.StorageCacheType;



public final class MySimulation {

    public static class RunStats {
        double maxPowerUsage;
        double totalTime;
    }

    // Non-instantiable
    private MySimulation() {
        throw new AssertionError();
    }

    public static interface Args {
        @Option String getOutputDirBase();

        @Option String getVmFile();

        @Option String getDagFileName();

        @Option(defaultValue="100.0001") List<Double> getPowerCapValues();
        @Option(defaultValue="0.0") List<Double> getPowerCapTimes();

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

        VMType vmType = (new VMTypeLoader()).determineVMTypeFromFile(args.getVmFile());


        // Run with an "infinite" power cap
        // ============================================================
        RunStats maxPowerRun = null;
        {
            PiecewiseConstantFunction powerCap =
                    new PiecewiseConstantFunction(0.0);
            powerCap.addJump(0.0, 1e100);

            String dir = args.getOutputDirBase() + File.separator + "nopowercap" + File.separator;
            (new File(dir)).mkdir();

            Planner planner = new HeftPlanner(asList(vmType));
            maxPowerRun = runTest(args.getDagFileName(), dir, vmType, powerCap, planner);
        }

        // Run with power caps based on maxPowerUsage
        // ============================================================
        List<Double> powerConstraints = asList(0.2, 0.5, 0.7);
        for (double powerConstraint : powerConstraints)
        {

            // As a baseline power use half the maximum useful power
            double basePower = maxPowerRun.maxPowerUsage / 2;

            // So the time to completion should be around doubled
            double timeEst = maxPowerRun.totalTime * 2;

            // Now make a varying power cap with a power supply dip in the
            // middle ~1/3 of the time.
            PiecewiseConstantFunction powerCap =
                    new PiecewiseConstantFunction(0.0);
            powerCap.addJump(0.0, basePower);
            powerCap.addJump(timeEst/3, basePower*powerConstraint);
            powerCap.addJump(2*timeEst/3, basePower);

            // Make the directory
            String dir = args.getOutputDirBase() + File.separator
                    + Double.toString(powerConstraint) + File.separator;
            (new File(dir)).mkdir();

            // and run it
            Planner planner = new PowerCappedPlanner(powerCap, new HeftPlanner());
            runTest(args.getDagFileName(), dir, vmType, powerCap, planner);
        }
    }

    public static RunStats runTest(String dagFileName,
            String outputDirName,
            VMType vmType,
            PiecewiseConstantFunction powerCap,
            Planner planner) {

        // For my purposes I'm not interested in (monetary) budget or a
        // deadline.
        double budget = 1e50;
        double deadline = 1e50;

        String outputFileName = outputDirName + "out.log";
        String powerFileName = outputDirName + "power.log";

        // Get the dag
        DAG dag = parseDag(dagFileName);

        // Make CloudSim object
        OutputStream logStream = getLogOutputStream(outputFileName);
        CloudSimWrapper cloudsim = new CloudSimWrapper(logStream);
        cloudsim.setLogsEnabled(true);
        Log.disable(); // We do not need Cloudsim's logs. We have our own.
        cloudsim.init();

        // Create storage manager (and register it with cloudsim somehow).
        // Use trivial storage simulation only
        StorageSimulationParams simulationParams = new StorageSimulationParams();
        simulationParams.setStorageCacheType(StorageCacheType.VOID);
        simulationParams.setStorageType(StorageType.VOID);
        StorageManagerFactory.createStorage(simulationParams, cloudsim);

        // Initial logs
        cloudsim.log("budget = " + budget);
        cloudsim.log("deadline = " + deadline);
        logWorkflowsDescription(dag, dagFileName, cloudsim);

        // Build our cloud
        Cloud cloud = new Cloud(cloudsim);


        // Build and plan the algorithm
        // ============================================================

        Provisioner provisioner = new NullProvisioner();

        StaticHeterogeneousAlgorithm staticAlgo =
                new StaticHeterogeneousAlgorithm.Builder(asList(dag), planner, cloudsim)
                .budget(budget)
                .deadline(deadline)
                .addInitialVMs(asList(vmType))
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


        // Log power usage
        // ============================================================
        PiecewiseConstantFunction powerUsed =
                algorithmStatistics.getPowerUsage();
        PiecewiseConstantFunction powerGap
                = powerCap.minus(algorithmStatistics.getPowerUsage());

        PrintWriter powerLog = null;
        try {
            powerLog = new PrintWriter(powerFileName, "UTF-8");
            // powerLog.print(pythonFormatPFunc("power gap", powerGap));
            powerLog.print(pythonFormatPFunc("power cap", powerCap));
            powerLog.print(pythonFormatPFunc("power used", powerUsed));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } finally {
            powerLog.close();
        }


        RunStats stats = new RunStats();
        stats.maxPowerUsage = Collections.max(powerUsed.jumpValues());
        stats.totalTime = algorithmStatistics.getLastJobFinishTime();

        return stats;

    }


    private static void logWorkflowsDescription(DAG dag, String dagFileName,
            CloudSimWrapper cloudsim) {

        String workflowDescription =
                String.format("Workflow %s, priority = %d, filename = %s",
                        dag.getId(),
                        0,
                        dagFileName);

        cloudsim.log(workflowDescription);
    }

    private static <T,U> String pythonFormatMap(Collection<Map.Entry<T,U>> map) {
        String a = "{";
        for (Map.Entry<T,U> e : map) {
            a += e.getKey().toString() + ":" + e.getValue().toString() + ", ";
        }
        a += "}";
        return a;
    }

    private static String pythonFormatPFunc(String label, PiecewiseConstantFunction f) {
        String mapAsString = pythonFormatMap(f.jumps());
        return String.format("('%s', %f, %s)\n",
                label, f.getInitialValue(), mapAsString);
    }

    /**
     * Returns output stream for logs for current simulation.
     */
    private static OutputStream getLogOutputStream(String outputFileName) {
        try {
            return new FileOutputStream(new File(outputFileName));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    // Load dags from files
    private static DAG parseDag(String dagFileName) {
        DAG dag = null;
        File dagFile = new File(dagFileName);
        dag = DAGParser.parseDAG(dagFile);

        dag.setId("0");
        return dag;
    }

}
