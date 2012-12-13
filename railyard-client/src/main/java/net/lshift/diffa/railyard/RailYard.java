package net.lshift.diffa.railyard;

import net.lshift.diffa.events.ChangeEvent;

public interface RailYard {

  void postChanges(String space, String endpoint, Iterable<ChangeEvent> events);
  Question getNextQuestion(String space, String endpoint);
  Question getNextQuestion(String space, String endpoint, Question lastQuestion, Iterable<Answer> answers);
}
