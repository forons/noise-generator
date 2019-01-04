package eu.unitn.disi.db.noise.generators;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public enum Edit {
  DELETION, SUBSTITUTION, INSERTION;
  private static final List<Edit> VALUES = Collections
      .unmodifiableList(Arrays.asList(Edit.values()));
  private static final int SIZE = VALUES.size();
  private static final Random rnd = new Random();

  public static Edit randomEDIT() {
    return VALUES.get(rnd.nextInt(SIZE));
  }
}
