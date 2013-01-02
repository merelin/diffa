package net.lshift.diffa.railyard;

import net.lshift.diffa.events.ChangeEvent;
import net.lshift.diffa.interview.Answer;
import net.lshift.diffa.interview.Question;

public interface RailYard {

  void postChanges(String space, String endpoint, Iterable<ChangeEvent> events);
  Question getNextQuestion(String space, String endpoint);
  Question getNextQuestion(String space, String endpoint, Question lastQuestion, Iterable<Answer> answers);
}
