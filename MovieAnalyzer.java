import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MovieAnalyzer {
  public static class Movie {
    private String seriesTitle;
    private int releaseYear;
    private String certificate;
    private int runtime;
    private String[] genre;
    private double imdbRating;
    private String overview;
    private int metaScore;
    private String director;
    private String star1;
    private String star2;
    private String star3;
    private String star4;
    private int noOfVotes;
    private int gross;

    public Movie(String info) {
      String[] arr = info.trim().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
      seriesTitle = arr[1].charAt(0) == '\"' ? arr[1].substring(1, arr[1].length() - 1) : arr[1];
      releaseYear = arr[2].isEmpty() ? -1 : Integer.parseInt(arr[2]);
      certificate = arr[3];
      String runtimeNum = arr[4].isEmpty() ? "" : arr[4].split(" ")[0];
      runtime = runtimeNum.isEmpty() ? -1 : Integer.parseInt(runtimeNum);
      if (arr[5].charAt(0) == '\"')
        arr[5] = arr[5].substring(1, arr[5].length() - 1);
      genre = arr[5].isEmpty() ? null : arr[5].split(", ");
      imdbRating = Float.parseFloat(arr[6].isEmpty() ? "-1" : arr[6]);
      overview = arr[7].isEmpty() ? "" : arr[7].charAt(0) == '\"' ? arr[7].substring(1, arr[7].length() - 1) : arr[7];
      metaScore = arr[8].isEmpty() ? -1 : Integer.parseInt(arr[8]);
      director = arr[9];
      star1 = arr[10];
      star2 = arr[11];
      star3 = arr[12];
      star4 = arr[13];
      noOfVotes = arr[14].isEmpty() ? -1 : Integer.parseInt(arr[14]);
      String grossNum = arr[15].isEmpty() ? "" : arr[15].substring(1, arr[15].length() - 1).replace(",", "");
      gross = grossNum.isEmpty() ? -1 : Integer.parseInt(grossNum);
    }

    public String getSeriesTitle() {
      return seriesTitle;
    }

    public int getReleaseYear() {
      return releaseYear;
    }

    public String getCertificate() {
      return certificate;
    }

    public int getRuntime() {
      return runtime;
    }

    public String[] getGenre() {
      return genre;
    }

    public double getImdbRating() {
      return imdbRating;
    }

    public String getOverview() {
      return overview;
    }

    public int getMetaScore() {
      return metaScore;
    }

    public String getDirector() {
      return director;
    }

    public String getStar1() {
      return star1;
    }

    public String getStar2() {
      return star2;
    }

    public String getStar3() {
      return star3;
    }

    public String getStar4() {
      return star4;
    }

    public int getNoOfVotes() {
      return noOfVotes;
    }

    public int getGross() {
      return gross;
    }
  }

  private static String dataset_path;

  public MovieAnalyzer(String dataset_path) {
    MovieAnalyzer.dataset_path = dataset_path;
  }

  public static Stream<Movie> readMovies() throws IOException {
    return Files.lines(Paths.get(dataset_path), StandardCharsets.UTF_8)
            .skip(1)
            .map(Movie::new);
  }

  public Map<Integer, Integer> getMovieCountByYear() throws IOException {
    Stream<Movie> movies = readMovies();
    Map<Integer, Long> middleResult =
            movies
                    .collect(Collectors.groupingBy(Movie::getReleaseYear, Collectors.counting()));
    Map<Integer, Integer> finalResult = new TreeMap<>(Comparator.reverseOrder());
    middleResult.
            entrySet()
            .stream()
            .filter(e -> e.getKey() != -1)
            .forEach(e -> finalResult.put(e.getKey(), e.getValue().intValue()));
    return finalResult;
  }

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
              if (e1.getValue() > e2.getValue())
                return -1;
              else if (e1.getValue() < e2.getValue())
                return 1;
              else
                return e1.getKey().compareTo(e2.getKey());
            })
            .forEachOrdered(e -> finalResult.put(e.getKey(), e.getValue()));
    return finalResult;
  }

  public Map<List<String>, Integer> getCoStarCount() throws IOException {
    Stream<Movie> movies = readMovies();
    Map<List<String>, Integer> result = new HashMap<>();
    movies.forEach(e -> {
      String[] stars = {e.star1, e.star2, e.star3, e.star4};
      Arrays.sort(stars);
      updateCoStarMap(result, stars[0], stars[1]);
      updateCoStarMap(result, stars[0], stars[2]);
      updateCoStarMap(result, stars[0], stars[3]);
      updateCoStarMap(result, stars[1], stars[2]);
      updateCoStarMap(result, stars[1], stars[3]);
      updateCoStarMap(result, stars[2], stars[3]);
    });
    return result;
  }

  private void updateCoStarMap(Map<List<String>, Integer> map, String star1, String star2) {
    List<String> list = new ArrayList<>();
    list.add(star1);
    list.add(star2);
    map.compute(list, (k, v) -> v == null ? 1 : v + 1);
  }

  public List<String> getTopMovies(int top_k, String by) throws Exception {
    if (by.equals("runtime")) {
      return readMovies()

              .filter(e -> e.getRuntime() != -1)
              .sorted(Comparator.comparing(Movie::getRuntime, Comparator.reverseOrder())
                      .thenComparing(Movie::getSeriesTitle))
              .limit(top_k)
              .map(Movie::getSeriesTitle)
              .toList();
    }
    if (by.equals("overview")) {
      return readMovies()
              .filter(x -> !x.overview.isEmpty() && !x.seriesTitle.isEmpty())
              .sorted((o1, o2) -> {
                if (o1.overview.length() > o2.overview.length())
                  return -1;
                if (o1.overview.length() < o2.overview.length())
                  return 1;
                return o1.seriesTitle.compareTo(o2.seriesTitle);
              })
              .limit(top_k)
              .map(Movie::getSeriesTitle)
              .toList();
    }
    throw new Exception(String.format("Method getTopMovies(), Line %d: Given criterion doesn't exist.",
            Thread.currentThread().getStackTrace()[1].getLineNumber()));
  }

  public List<String> getTopStars(int top_k, String by) throws Exception {
    if (by.equals("rating")) {
      Map<String, Integer> appearTimes = new HashMap<>();
      Map<String, Double> total = new HashMap<>();
      readMovies()
              .filter(x -> !x.seriesTitle.isEmpty() && x.imdbRating != -1 && !x.star1.isEmpty() && !x.star2.isEmpty() && !x.star3.isEmpty() && !x.star4.isEmpty())
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
      return calculateListByAverage(top_k, appearTimes, total);
    }
    if (by.equals("gross")) {
      Map<String, Integer> appearTimes = new HashMap<>();
      Map<String, Double> total = new HashMap<>();
      readMovies()
              .filter(x -> !x.seriesTitle.isEmpty() && x.gross != -1 && !x.star1.isEmpty() && !x.star2.isEmpty() && !x.star3.isEmpty() && !x.star4.isEmpty())
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
      return calculateListByAverage(top_k, appearTimes, total);
    }
    throw new Exception(String.format("Method getTopMovies(), Line %d: Given criterion doesn't exist.",
            Thread.currentThread().getStackTrace()[1].getLineNumber()));
  }

  private List<String> calculateListByAverage(int top_k, Map<String, Integer> appearTimes, Map<String, Double> total) {
    Map<String, Double> average = new HashMap<>();
    appearTimes.forEach((k, v) -> average.put(k, 1.0 * total.get(k) / v));
    return average
            .entrySet()
            .stream()
            .sorted((e1, e2) -> {
              if (e1.getValue() < e2.getValue())
                return 1;
              if (e1.getValue() > e2.getValue())
                return -1;
              return e1.getKey().compareTo(e2.getKey());
            })
            .limit(top_k)
            .map(Map.Entry::getKey)
            .toList();
  }

  public List<String> searchMovies(String genre, float min_rating, int max_runtime) throws IOException {
    return readMovies()
            .filter(x -> x.genre != null && x.imdbRating != -1 && x.runtime != -1)
            .filter(x -> Arrays.asList(x.genre).contains(genre) && x.imdbRating >= min_rating && x.runtime <= max_runtime)
            .sorted(Comparator.comparing(Movie::getSeriesTitle))
            .map(Movie::getSeriesTitle)
            .toList();
  }
}