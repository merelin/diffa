package net.lshift.diffa.versioning;

public class EntityNotFoundException extends RuntimeException{

  public EntityNotFoundException(String message) {
    super(message);
  }
}
