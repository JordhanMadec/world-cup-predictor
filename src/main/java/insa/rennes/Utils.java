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

    public static String getHost(int edition) {
        switch (edition) {
            case 1930: return "Uruguay";
            case 1934: return "Italy";
            case 1938: return "France";
            case 1950: return "Brazil";
            case 1954: return "Switzerland";
            case 1958: return "Sweden";
            case 1962: return "Chile";
            case 1966: return "England";
            case 1970: return "Mexico";
            case 1974: return "Germany";
            case 1978: return "Argentina";
            case 1982: return "Spain";
            case 1986: return "Mexico";
            case 1990: return "Italy";
            case 1994: return "USA";
            case 1998: return "France";
            case 2002: return "Japan";
            case 2006: return "Germany";
            case 2010: return "South Africa";
            case 2014: return "Brazil";
            default: return "";
        }
    }
}
