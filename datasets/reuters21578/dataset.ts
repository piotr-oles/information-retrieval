import {
  ElementInfo,
  PullParser,
  PullResult,
} from "https://deno.land/x/xmlp/mod.ts";

export interface Reuters21578Document {
  id: number;
  title: string;
  body?: string;
  date?: Date;
  topics: string[];
  places: string[];
  people: string[];
  orgs: string[];
  exchanges: string[];
  companies: string[];
  split?: "TRAIN" | "TEST";
}

export class Reuters21578Dataset {
  constructor() {}

  async *read() {
    const SEGMENTS_NUMBER = 22;
    for (let i = 0; i < SEGMENTS_NUMBER; i++) {
      const index = i.toString().padStart(3, "0");
      const path = `${import.meta.dirname}/reut2-${index}.sgm`;

      yield* this.readDocuments(path);
    }
  }

  private async *readDocuments(path: string) {
    for await (const xml of this.readXMLs(path)) {
      const parser = new PullParser();
      const builder = new Reuters21578DocumentBuilder();

      for (const event of parser.parse(xml)) {
        builder.consume(event);
      }

      yield builder.build();
    }
  }

  private async *readXMLs(path: string) {
    using file = await Deno.open(path, { read: true });

    const decoder = new TextDecoder();
    let leftover = "";

    for await (const bytes of file.readable) {
      const chunk = decoder.decode(bytes);
      const content = leftover + chunk;
      const matches = content.matchAll(/<REUTERS[\s\S]*?<\/REUTERS>/g);

      for (const match of matches) {
        yield match[0];
      }

      leftover = content.slice(
        content.lastIndexOf("</REUTERS>") + "</REUTERS>".length
      );
    }
  }
}

class Reuters21578DocumentBuilder {
  #document: Reuters21578Document;
  #elements: (ElementInfo | undefined)[];
  get #element(): ElementInfo | undefined {
    return this.#elements[this.#elements.length - 1];
  }

  constructor() {
    this.#elements = [];
    this.#document = {
      id: -1,
      title: "",
      topics: [],
      places: [],
      people: [],
      orgs: [],
      exchanges: [],
      companies: [],
    };
  }

  consume(event: PullResult): void {
    switch (event.name) {
      case "start_element":
        this.handleStartElement(event);
        break;
      case "end_element":
        this.handleEndElement();
        break;
      case "text":
        this.handleText(event);
        break;
    }
  }

  build(): Reuters21578Document {
    return this.#document;
  }

  private handleStartElement(event: PullResult) {
    this.#elements.push(event.element);

    if (event.element && event.element.qName === "REUTERS") {
      this.#document.id = Number(
        event.element.attributes.find(
          (attribute) => attribute.qName === "NEWID"
        )?.value
      );
      this.#document.split =
        event.element.attributes.find(
          (attribute) => attribute.qName === "LEWISSPLIT"
        )?.value === "TEST"
          ? "TEST"
          : "TRAIN";
    }
  }

  private handleEndElement() {
    this.#elements.pop();
  }

  private handleText(event: PullResult) {
    const element = this.#element;
    switch (element?.qName) {
      case "TITLE":
        this.handleTitleElement(event.text);
        break;
      case "BODY":
        this.handleBodyElement(event.text);
        break;
      case "TEXT":
        this.handleTextElement(event.text);
        break;
      case "DATE":
        this.handleDateElement(event.text);
        break;
      case "D": {
        this.handleDElement(element, event.text);
        break;
      }
    }
  }

  private handleTitleElement(text: string | undefined) {
    if (text) {
      this.#document.title = text;
    }
  }

  private handleBodyElement(text: string | undefined) {
    if (text) {
      this.#document.body = text;
    }
  }

  private handleTextElement(text: string | undefined) {
    if (text && !this.#document.body) {
      this.#document.body = text;
    }
  }

  private handleDateElement(text: string | undefined) {
    if (text) {
      try {
        this.#document.date = new Date(text);
      } catch (error) {
        // ignore - couldn't parse date
      }
    }
  }

  private handleDElement(element: ElementInfo, text: string | undefined) {
    switch (element.parent?.qName) {
      case "TOPICS":
        this.handleTopicDElement(text);
        break;
      case "PLACES":
        this.handlePlaceDElement(text);
        break;
      case "PEOPLE":
        this.handlePeopleDElement(text);
        break;
      case "ORGS":
        this.handleOrgsDElement(text);
        break;
      case "EXCHANGES":
        this.handleExchangesDElement(text);
        break;
      case "COMPANIES":
        this.handleCompaniesDElement(text);
        break;
    }
  }

  private handleTopicDElement(text: string | undefined) {
    if (text) {
      this.#document.topics.push(text);
    }
  }

  private handlePlaceDElement(text: string | undefined) {
    if (text) {
      this.#document.places.push(text);
    }
  }

  private handlePeopleDElement(text: string | undefined) {
    if (text) {
      this.#document.people.push(text);
    }
  }

  private handleOrgsDElement(text: string | undefined) {
    if (text) {
      this.#document.orgs.push(text);
    }
  }

  private handleExchangesDElement(text: string | undefined) {
    if (text) {
      this.#document.exchanges.push(text);
    }
  }

  private handleCompaniesDElement(text: string | undefined) {
    if (text) {
      this.#document.companies.push(text);
    }
  }
}
