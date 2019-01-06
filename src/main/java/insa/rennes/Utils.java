package insa.rennes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

public class Utils {
    private static final int[] EDITIONS = {
        1930, 1934, 1938, 1950, 1954, 1958, 1962, 1966, 1970, 1974, 1978, 1982, 1986, 1990, 1994, 1998, 2002, 2006, 2010, 2014, 2018
    };

    public static int getWorldCupEdition(int year) {
        for (int edition: EDITIONS) {
            if (year <= edition - 1 && edition - 1 - year < Settings.YEARS_BEFORE_EDITION) return edition;
        }

        return 0;
    }

    public static int[] getEDITIONS() {
        return EDITIONS;
    }

    public static HashSet<String> getAllCountries() {
        String csvFile = Settings.internationalResultsPath;
        String line = "";
        String cvsSplitBy = ",";
        HashSet<String> countries = new HashSet<>();

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            while ((line = br.readLine()) != null) {
                countries.add(line.split(cvsSplitBy)[1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return countries;
    }
}
