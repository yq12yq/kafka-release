package kafka.log;

import java.io.File;

public class TestMMappedFile {

    public static void main(String[] args) throws Exception {
        if(args.length < 2) {
            System.err.println("Usage: <--validate>  <directory> or <--create> <directory> <num-of-files>");
            return;
        }
        File dir = new File(args[1]);

        if(args[0].equals("--validate")) {
            File[] files = dir.listFiles();
            for(File file: files) {
                OffsetIndex offsetIndex = new OffsetIndex(file, 0, 10485760);
                if(offsetIndex.entries() != 10000) {
                    System.err.println(file.getAbsolutePath() + " has only " + offsetIndex.entries() + " entries, expected 10000");
                }

                for(int j = 0 ;j < 10000 ; j ++) {
                    OffsetPosition entry = offsetIndex.entry(j);
                    if(entry.offset() != j || entry.position() != j) {
                        System.err.println(file.getAbsolutePath() + " has mismatched Entry expected: " + new OffsetPosition(j, j) + " got " + entry);
                    }
                }
            }
            System.out.println("Validation passed for all files");
            return;
        }

        int numFiles = Integer.parseInt(args[2]);
        for(int i =0;i < numFiles ; i++) {
            File file = new File(dir.getAbsolutePath() + File.separator + i + ".index");
            if(file.exists()) {
                file.delete();
            }
            OffsetIndex idx = new OffsetIndex(file, 0, 10485760);
            for(int j = 0 ;j < 10000 ; j ++) {
                idx.append(j, j);
            }
            idx.flush();
            idx.close();
        }
        System.out.println("Finieshed creating all index files.");
    }
}
