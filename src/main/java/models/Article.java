package models;

import java.io.Serializable;

public class Article implements Serializable {
  private int id;
  private String url;
  private String title;
  private String content;

  public Article() {
  }

  public Article(int id, String url, String title, String content) {
    this();
    setId(id);
    setContent(content);
    setTitle(title);
    setUrl(url);
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  @Override
  public String toString() {
    return "Article{" + "id=" + id + ", title='" + title + '\'' + '}';
  }
}
