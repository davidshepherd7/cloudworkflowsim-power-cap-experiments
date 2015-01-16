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
import cws.core.algorithms.heterogeneous.DynamicHeterogeneousAlgorithm;
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
import cws.core.dag.algorithms.CriticalPath;
import cws.core.dag.algorithms.TopologicalOrder;
import cws.core.engine.Environment;
import cws.core.engine.EnvironmentFactory;
import cws.core.exception.IllegalCWSArgumentException;
import cws.core.VMFactory;

import cws.core.Scheduler;
import cws.core.scheduler.EnsembleDynamicScheduler;

import cws.core.Provisioner;
import cws.core.provisioner.NullProvisioner;
import cws.core.provisioner.PowerCappedProvisioner;

import cws.core.storage.StorageManagerStatistics;
import cws.core.storage.StorageManagerFactory;
import cws.core.storage.StorageManager;
import cws.core.simulation.StorageSimulationParams;
import cws.core.simulation.StorageType;
import cws.core.simulation.StorageCacheType;



public final class FCFSPowerCapped {

    public static class RunStats {
        double maxPowerUsage;
        double totalEnergyConsumed;

        double makespan;
        double optimalMakespan;

        String application;
        int size;
        double powerDipFraction;
    }

    // Non-instantiable
    private FCFSPowerCapped() {
        throw new AssertionError();
    }

    public static interface Args {
        @Option String getOutputDirBase();

        @Option String getVmFile();

        @Option String getDagFileName();

        @Option String getApplication();

        @Option Integer getSize();

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

        final VMType vmType = (new VMTypeLoader()).determineVMTypeFromFile(args.getVmFile());

        // Get the dag
        final DAG dag = parseDag(args.getDagFileName());


        // Estimate time and power usage
        // ============================================================
        // (for constructing interesting power cap functions)

        // Compute a lower bound for the makespan based on the critical
        // path computation time
        final double timeEst = criticalPathMakespanBound(dag, vmType);

        // Get estimate of energy consumed by counting the number of
        // instructions in the DAG and getting energy per instruction from
        // the vm, then powerEst is chosen such that energy provided by
        // time timeEst is approximately the energy consumed by the
        // uncapped version.
        final double joulesPerMInstructions = vmType.getPowerConsumption() / vmType.getMips();
        final double totalEnergyNeeded = dag.getTotalSize() * joulesPerMInstructions;
        final double powerEst = totalEnergyNeeded / timeEst;

        final double basePower = powerEst;

        //??ds Should I make sure the power is always more than the power
        //for one VM?

        // Run with power caps which dip in the middle
        // ============================================================
        final List<Double> powerConstraints = asList(0.2, 0.5, 0.7);
        for (double powerConstraint : powerConstraints)
        {
            // Make a varying power cap with a power supply dip in the
            // middle ~1/3 of the time.
            PiecewiseConstantFunction powerCap =
                    new PiecewiseConstantFunction(0.0);
            powerCap.addJump(0.0, basePower);
            powerCap.addJump(timeEst/3, basePower*powerConstraint);
            powerCap.addJump(2*timeEst/3, basePower);

            // Make the directory
            final String dir = args.getOutputDirBase() + File.separator
                    + Double.toString(powerConstraint) + File.separator;
            (new File(dir)).mkdir();

            // and run it
            RunStats data = runTest(dag, dir, vmType, powerCap,
                    args.getDagFileName());

            data.application = args.getApplication();
            data.size = args.getSize();
            data.powerDipFraction = powerConstraint;

            // write data needed for SLR-like plot
            writeSLRPlotData(dir, data);
        }
    }

    private static void writeSLRPlotData(String dir, RunStats data) {
        File file = new File(dir, "slr_plot_data");

        PrintWriter writer = null;
        try {
            writer = new PrintWriter(file.toString(), "UTF-8");
            writer.printf("'application' '%s'\n", data.application);
            writer.printf("'size' '%s'\n", data.size);
            writer.printf("'optimalMakespan' %f\n", data.optimalMakespan);
            writer.printf("'makespan' %f\n", data.makespan);
            writer.printf("'powerDipFraction' %f\n", data.powerDipFraction);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } finally {
            writer.close();
        }
    }

    public static RunStats runTest(DAG dag,
            String outputDirName,
            VMType vmType,
            PiecewiseConstantFunction powerCap,
            String dagFileName) {

        // For my purposes I'm not interested in (monetary) budget or a
        // deadline.
        final double budget = 1e50;
        final double deadline = 1e50;

        final String outputFileName = outputDirName + "out.log";
        final String powerFileName = outputDirName + "power.log";

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

        Provisioner provisioner = new PowerCappedProvisioner(cloudsim, powerCap, asList(vmType));
        provisioner.setCloud(cloud);

        Algorithm algorithm =
                new DynamicHeterogeneousAlgorithm(budget, deadline, asList(dag), cloudsim);
        Scheduler scheduler = new EnsembleDynamicScheduler(cloudsim);

        WorkflowEngine engine = new WorkflowEngine(provisioner, scheduler,
                budget, deadline, cloudsim);

        algorithm.setWorkflowEngine(engine);
        algorithm.setCloud(cloud);

        // Run
        algorithm.simulate();


        // Generate stats about how well the job did
        // ============================================================
        AlgorithmStatistics algorithmStatistics = algorithm.getAlgorithmStatistics();
        final double planningTime = algorithm.getPlanningnWallTime() / 1.0e9;
        final double simulationTime = cloudsim.getSimulationWallTime() / 1.0e9;


        // Log power usage
        // ============================================================
        final PiecewiseConstantFunction powerUsed =
                algorithmStatistics.getPowerUsage();
        final PiecewiseConstantFunction powerGap
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

        final double makespan = algorithmStatistics.getLastJobFinishTime();

        final double om = optimalMakespan(powerCap, vmType, dag);
        System.out.printf("optimal makespan: %f\n", om);
        System.out.printf("actual makespan: %f\n", makespan);
        System.out.printf("ratio: %f\n\n", makespan/om);


        RunStats stats = new RunStats();
        // stats.maxPowerUsage = Collections.max(powerUsed.jumpValues());
        stats.makespan = algorithmStatistics.getLastJobFinishTime();
        // stats.totalEnergyConsumed = powerUsed.integral(0.0, makespan);
        stats.optimalMakespan = om;

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
        final String mapAsString = pythonFormatMap(f.jumps());
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

    /** Load dags from file.
     */
    private static DAG parseDag(String dagFileName) {
        final File dagFile = new File(dagFileName);
        DAG dag = DAGParser.parseDAG(dagFile);
        dag.setId("0");
        return dag;
    }

    /** Compute a lower bound on the makespan based on summing the
     * computation time for tasks in the critical path of the DAG.
     */
    private static double criticalPathMakespanBound(DAG dag, VMType vmType) {
        // Could extend to heterogeneous VMs by using SLR definition from
        // HEFT paper (Topcuoglu2002 eq 11).
        final CriticalPath cp = new CriticalPath(new TopologicalOrder(dag), vmType);
        return cp.getCriticalPathLength();
    }

    /**
     * Compute an optimal makespan based on the amount of computation that
     * can be done within the power cap. Based on the simplifying
     * assumption that the optimal makespan is after the final change in
     * the power cap.
     */
    private static double optimalMakespan(PiecewiseConstantFunction powerCap,
            VMType vmType, DAG dag) {

        // Could extend to heterogeneous VMs (multiple VM types) by taking
        // the mean M instructions per joule.

        final Map.Entry<Double, Double> lastJump = powerCap.getFinalJump();
        final double lastJumpTime = lastJump.getKey();
        final double lastJumpPower = lastJump.getValue();

        final double mInstructionsPerJoule = vmType.getMips() / vmType.getPowerConsumption();
        final double totalMInstructionsNeeded = dag.getTotalSize();

        // Compute how many instructions we could have completed before the
        // last jump in the power cap.
        final double baseEnergy = powerCap.integral(0.0, lastJumpTime);
        final double baseInstructions = baseEnergy * mInstructionsPerJoule;

        // So from now on the power is fixed, compute time to finish with
        // this power.
        final double remainingInstructions = totalMInstructionsNeeded - baseInstructions;
        final double remainingEnergy = remainingInstructions / mInstructionsPerJoule;
        final double remainingTime = remainingEnergy / lastJumpPower;
        final double optimalMakespan = lastJumpTime + remainingTime;

        // We assumed that we are not finished until after the final jump,
        // check that this is true.
        if(remainingInstructions < 0) {
            throw new RuntimeException("This function assumes that the final change in the power cap is earlier than the makespan, but this was not true.");
        }

        return optimalMakespan;
    }


}
