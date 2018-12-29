package insa.rennes;

public class Utils {
    private static final int[] EDITIONS = {
        1930, 1934, 1938, 1950, 1954, 1958, 1962, 1966, 1970, 1974, 1978, 1982, 1986, 1990, 1994, 1998, 2002, 2006, 2010, 2014, 2018
    };

    public static int getWorldCupEdition(int year) {
        for (int edition: EDITIONS) {
            if (year <= edition && edition - year < 4) return edition;
        }

        return 0;
    }
}
