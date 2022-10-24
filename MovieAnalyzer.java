import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An assignment for Sustech Java2.
 *
 * @author : Lin Yuhang
 * @SID: 12010903
 */
public class MovieAnalyzer {

  /**
   * Movie class is design for easier operation of stream in this assignment.
   *
   * @author : Lin Yuhang
   * @SID: 12010903
   */
  public static class Movie {
    private String seriesTitle;
    private int releaseYear;
	@@ -24,27 +46,49 @@ public static class Movie {
    private int noOfVotes;
    private int gross;


    /**
     * The constructor of class Movie.
     *
     * @param: info: a line in csv file
     * @return: None.
     * @Author: Lin Yuhang
     * @SID: 12010903
     */
    public Movie(String info) {
      String[] arr = info.trim().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
      seriesTitle = arr[1].charAt(0) == '\"' ? arr[1].substring(1, arr[1].length() - 1) : arr[1];
      releaseYear = arr[2].isEmpty() ? -1 : Integer.parseInt(arr[2]);
      certificate = arr[3];
      String runtimeNum = arr[4].isEmpty() ? "" : arr[4].split(" ")[0];
      runtime = runtimeNum.isEmpty() ? -1 : Integer.parseInt(runtimeNum);
      if (arr[5].charAt(0) == '\"') {
        arr[5] = arr[5].substring(1, arr[5].length() - 1);
      }
      genre = arr[5].isEmpty() ? null : arr[5].split(", ");
      imdbRating = Float.parseFloat(arr[6].isEmpty() ? "-1" : arr[6]);
      if (arr[7].isEmpty()) {
        overview = "";
      } else {
        if (arr[7].charAt(0) == '\"') {
          overview = arr[7].substring(1, arr[7].length() - 1);
        } else {
          overview = arr[7];
        }
      }
      metaScore = arr[8].isEmpty() ? -1 : Integer.parseInt(arr[8]);
      director = arr[9];
      star1 = arr[10];
      star2 = arr[11];
      star3 = arr[12];
      star4 = arr[13];
      noOfVotes = arr[14].isEmpty() ? -1 : Integer.parseInt(arr[14]);
      if (arr[15].isEmpty()) {
        gross = -1;
      } else {
        String grossNum = arr[15].substring(1, arr[15].length() - 1).replace(",", "");
        gross = grossNum.isEmpty() ? -1 : Integer.parseInt(grossNum);
      }
    }

    public String getSeriesTitle() {
	@@ -106,63 +150,109 @@ public int getNoOfVotes() {
    public int getGross() {
      return gross;
    }

    public int getOverviewLength() {
      return overview.length();
    }
  }

  private static String dataset_path;

  public MovieAnalyzer(String datasetPath) {
    MovieAnalyzer.dataset_path = datasetPath;
  }

  /**
   * Read csv file and create a stream.
   *
   * @Param: None
   * @return: a stream consists of lines in csv file
   * @Author: Lin Yuhang
   * @SID: 12010903
   */
  public static Stream<Movie> readMovies() throws IOException {
    return Files.lines(Paths.get(dataset_path), StandardCharsets.UTF_8)
        .skip(1)
        .map(Movie::new);
  }

  /**
   * Calculate number of movies released in each year.
   *
   * @param: None
   * @return: a < year, count > map,
  where the key is the year while the value is the number of movies released in that year
  The map should be sorted by descending order of year (i.e., from the latest to the earliest).
   * @Author: Lin Yuhang
   * @SID: 12010903
   */
  public Map<Integer, Integer> getMovieCountByYear() throws IOException {
    Stream<Movie> movies = readMovies();
    Map<Integer, Integer> finalResult = new TreeMap<>(Comparator.reverseOrder());
    movies
        .filter(x -> x.releaseYear != -1)
        .collect(Collectors.groupingBy(
            Movie::getReleaseYear,
            Collectors.counting()))
        .forEach((k, v) -> finalResult.put(k, v.intValue()));
    return finalResult;
  }

  /**
   * Get number of movies by genre.
   *
   * @param: None
   * @return: This method returns a < genre, count > map,
  where the key is the genre while the value is the number of movies in that genre.
  The map should be sorted by descending order of count
  (i.e., from the most frequent genre to the least frequent genre).
  If two genres have the same count,
  then they should be sorted by the alphabetical order of the genre names.
   * @Author: Lin Yuhang
   * @SID: 12010903
   */
  public Map<String, Integer> getMovieCountByGenre() throws IOException {
    Stream<Movie> movies = readMovies();
    Set<String> genres = new HashSet<>();

    movies.forEach(e -> genres.addAll(Arrays.asList(e.genre)));
    Map<String, Integer> middleResult = new TreeMap<>();
    genres
        .forEach(e -> middleResult.put(e, 0));
    movies = readMovies();
    movies.forEach(e -> {
      Arrays.stream(e.genre)
          .forEach(g -> middleResult.put(g, middleResult.get(g) + 1));
    });
    Map<String, Integer> finalResult = new LinkedHashMap<>();
    middleResult
        .entrySet()
        .stream()
        .sorted((e1, e2) -> {
          if (e1.getValue() > e2.getValue()) {
            return -1;
          } else if (e1.getValue() < e2.getValue()) {
            return 1;
          } else {
            return e1.getKey().compareTo(e2.getKey());
          }
        })
        .forEachOrdered(e -> finalResult.put(e.getKey(), e.getValue()));
    return finalResult;
  }

  /**
   * Movie count by co-stars.
   *
   * @param:  None
   * @return: This method returns a <[star1, star2], count> map,
  where the key is a list of names of the stars
  while the value is the number of movies that they have co-starred in.
  Note that the length of the key is 2 and
  the names of the stars should be sorted by alphabetical order in the list.
   * @Author: Lin Yuhang
   * @SID: 12010903
   */
  public Map<List<String>, Integer> getCoStarCount() throws IOException {
    Stream<Movie> movies = readMovies();
    Map<List<String>, Integer> result = new HashMap<>();
	@@ -186,98 +276,144 @@ private void updateCoStarMap(Map<List<String>, Integer> map, String star1, Strin
    map.compute(list, (k, v) -> v == null ? 1 : v + 1);
  }

  /**
   * Top movies.
   *
   * @param:
  topK: how many movies will remain
  by:
  by="runtime": the results should be movies sorted by descending order of runtime
  (from the longest movies to the shortest movies) .
  by="overview": the results should be movies sorted by descending order
  of the length of the overview
  (from movies with the longest overview to movies with the shortest overview).
   * @return: the top K movies (paramter top_k) by the given criterion (paramter by).
  Specifically,
  Note that the results should be a list of movie titles.
  If two movies have the same runtime or overview length,
  then they should be sorted by alphabetical order of their titles.
   * @Author: Lin Yuhang
   * @SID: 12010903
   */
  public List<String> getTopMovies(int topK, String by) throws Exception {
    if (by.equals("runtime")) {
      return readMovies()
          .filter(e -> e.getRuntime() != -1)
          .sorted(Comparator.comparing(Movie::getRuntime, Comparator.reverseOrder())
              .thenComparing(Movie::getSeriesTitle))
          .limit(topK)
          .map(Movie::getSeriesTitle)
          .toList();
    }
    if (by.equals("overview")) {
      return readMovies()
          .filter(x -> !x.overview.isEmpty() && !x.seriesTitle.isEmpty())
          .sorted(Comparator.comparing(Movie::getOverviewLength, Comparator.reverseOrder())
              .thenComparing(Movie::getSeriesTitle))
          .limit(topK)
          .map(Movie::getSeriesTitle)
          .toList();
    }
    throw new Exception(
        String.format("Method getTopMovies(), Line %d: Given criterion doesn't exist.",
            Thread.currentThread().getStackTrace()[1].getLineNumber()));
  }

  /**
   *  Top stars.
   *
   * @param:
   * @return:
   * @Author: Lin Yuhang
   * @SID: 12010903
   */
  public List<String> getTopStars(int topK, String by) throws Exception {
    if (by.equals("rating")) {
      Map<String, Integer> appearTimes = new HashMap<>();
      Map<String, Double> total = new HashMap<>();
      readMovies()
          .filter(x -> !x.seriesTitle.isEmpty())
          .filter(x -> x.imdbRating != -1)
          .forEach(e -> {
            appearTimes.compute(e.star1, (k, v) -> v == null ? 1 : v + 1);
            appearTimes.compute(e.star2, (k, v) -> v == null ? 1 : v + 1);
            appearTimes.compute(e.star3, (k, v) -> v == null ? 1 : v + 1);
            appearTimes.compute(e.star4, (k, v) -> v == null ? 1 : v + 1);
            total.compute(e.star1, (k, v) -> v == null ? e.imdbRating : v + e.imdbRating);
            total.compute(e.star2, (k, v) -> v == null ? e.imdbRating : v + e.imdbRating);
            total.compute(e.star3, (k, v) -> v == null ? e.imdbRating : v + e.imdbRating);
            total.compute(e.star4, (k, v) -> v == null ? e.imdbRating : v + e.imdbRating);
          });
      return calculateListByAverage(topK, appearTimes, total);
    }
    if (by.equals("gross")) {
      Map<String, Integer> appearTimes = new HashMap<>();
      Map<String, Double> total = new HashMap<>();
      readMovies()
          .filter(x -> !x.seriesTitle.isEmpty() && x.gross != -1)
          .forEach(e -> {
            appearTimes.compute(e.star1, (k, v) -> v == null ? 1 : v + 1);
            appearTimes.compute(e.star2, (k, v) -> v == null ? 1 : v + 1);
            appearTimes.compute(e.star3, (k, v) -> v == null ? 1 : v + 1);
            appearTimes.compute(e.star4, (k, v) -> v == null ? 1 : v + 1);
            total.compute(e.star1, (k, v) -> v == null ? e.gross : v + e.gross);
            total.compute(e.star2, (k, v) -> v == null ? e.gross : v + e.gross);
            total.compute(e.star3, (k, v) -> v == null ? e.gross : v + e.gross);
            total.compute(e.star4, (k, v) -> v == null ? e.gross : v + e.gross);
          });
      return calculateListByAverage(topK, appearTimes, total);
    }
    throw new Exception(
        String.format("Method getTopMovies(), Line %d: Given criterion doesn't exist.",
            Thread.currentThread().getStackTrace()[1].getLineNumber()));
  }

  private List<String> calculateListByAverage(int topK,
                                              Map<String, Integer> appearTimes,
                                              Map<String, Double> total) {
    Map<String, Double> average = new HashMap<>();
    appearTimes.forEach((k, v) -> average.put(k, 1.0 * total.get(k) / v));
    return average
        .entrySet()
        .stream()
        .sorted((e1, e2) -> {
          if (e1.getValue() < e2.getValue()) {
            return 1;
          }
          if (e1.getValue() > e2.getValue()) {
            return -1;
          }
          return e1.getKey().compareTo(e2.getKey());
        })
        .limit(topK)
        .map(Map.Entry::getKey)
        .toList();
  }

  /**
   * Search movies.
   *
   * @param:
  genre: genre of the movie
  min_rating: the rating of the movie should >= min_rating
  max_runtime: the runtime (min) of the movie should <= max_runtime
   * @return: Note that the results should be a list of movie titles that meet the given criteria,
  and sorted by alphabetical
  order of the titles.
   * @Author: Lin Yuhang
   * @SID: 12010903
   */
  public List<String> searchMovies(String genre,
                                   float minRating,
                                   int maxRuntime)
      throws IOException {
    return readMovies()
        .filter(x -> x.genre != null && x.imdbRating != -1 && x.runtime != -1)
        .filter(x -> Arrays.asList(x.genre).contains(genre))
        .filter(x -> x.imdbRating >= minRating)
        .filter(x -> x.runtime <= maxRuntime)
        .sorted(Comparator.comparing(Movie::getSeriesTitle))
        .map(Movie::getSeriesTitle)
        .toList();
  }
}