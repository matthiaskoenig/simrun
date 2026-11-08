import { mount } from "@vue/test-utils";
import { describe, expect, it } from "vitest";
import SchemaForm from "./SchemaForm.vue";

const schema = {
  properties: {
    n: { anyOf: [{ type: "integer" }, { type: "null" }], default: 7, title: "N" },
    extra: { additionalProperties: { type: "string" }, title: "Extra", type: "object" },
    names: { items: { type: "string" }, title: "Names", type: "array" },
  },
  type: "object",
};

describe("SchemaForm", () => {
  it("emits the integer typed and removes the key when cleared", async () => {
    const wrapper = mount(SchemaForm, { props: { schema, value: {} } });
    const input = wrapper.get("[data-field='n'] input");
    expect(input.attributes("placeholder")).toBe("7");
    await input.setValue("5");
    expect(wrapper.emitted("update:value")!.at(-1)).toEqual([{ n: 5 }]);
    await wrapper.setProps({ value: { n: 5 } });
    await input.setValue("");
    expect(wrapper.emitted("update:value")!.at(-1)).toEqual([{}]);
  });

  it("emits a list one item per line", async () => {
    const wrapper = mount(SchemaForm, { props: { schema, value: {} } });
    await wrapper.get("[data-field='names'] textarea").setValue("a\n\nb");
    expect(wrapper.emitted("update:value")!.at(-1)).toEqual([{ names: ["a", "b"] }]);
  });

  it("does not wrap the items of a list, so a long one stays on its line", () => {
    const wrapper = mount(SchemaForm, { props: { schema, value: {} } });
    expect(wrapper.get("[data-field='names'] textarea").attributes("wrap")).toBe("off");
    expect(wrapper.get("[data-field='extra'] textarea").attributes("wrap")).toBe("soft");
  });

  it("shows every item of a list, up to eight rows", async () => {
    const lines = (n: number) => Array.from({ length: n }, (_, i) => `a${i}`);
    const few = mount(SchemaForm, { props: { schema, value: { names: lines(2) } } });
    expect(few.get("[data-field='names'] textarea").attributes("rows")).toBe("3");
    const some = mount(SchemaForm, { props: { schema, value: { names: lines(4) } } });
    expect(some.get("[data-field='names'] textarea").attributes("rows")).toBe("5");
    const many = mount(SchemaForm, { props: { schema, value: { names: lines(20) } } });
    expect(many.get("[data-field='names'] textarea").attributes("rows")).toBe("8");
  });

  it("keeps invalid JSON text, shows the message and emits nothing", async () => {
    const wrapper = mount(SchemaForm, { props: { schema, value: {} } });
    const area = wrapper.get("[data-field='extra'] textarea");
    await area.setValue('{"a": ');
    expect(wrapper.emitted("update:value")).toBeUndefined();
    expect(wrapper.get("[data-field='extra']").text()).toContain("not valid JSON");
    expect((area.element as HTMLTextAreaElement).value).toBe('{"a": ');
    await area.setValue('{"a": "b"}');
    expect(wrapper.emitted("update:value")!.at(-1)).toEqual([{ extra: { a: "b" } }]);
  });

  it("says what an integer field refuses and emits nothing until it parses", async () => {
    const wrapper = mount(SchemaForm, { props: { schema, value: {} } });
    const input = wrapper.get("[data-field='n'] input");
    await input.setValue("2.5");
    expect(wrapper.emitted("update:value")).toBeUndefined();
    expect(wrapper.get("[data-field='n']").text()).toContain("a whole number");
    await input.setValue("3");
    expect(wrapper.emitted("update:value")!.at(-1)).toEqual([{ n: 3 }]);
    expect(wrapper.get("[data-field='n']").text()).not.toContain("a whole number");
  });

  it("shows the errors of a field under it, those of a nested field in its group", () => {
    const nested = {
      properties: {
        n: { title: "N", type: "integer" },
        opt: { properties: { tol: { title: "Tol", type: "number" } }, title: "Opt", type: "object" },
      },
      type: "object",
    };
    const wrapper = mount(SchemaForm, {
      props: { schema: nested, value: {}, errors: { n: ["Field required"], "opt.tol": ["Input should be a valid number"] } },
    });
    expect(wrapper.get("[data-field='n'] [data-field-error]").text()).toBe("Field required");
    expect(wrapper.get("[data-field='n'] input").classes()).toContain("border-red-400");
    expect(wrapper.get("[data-field='tol'] [data-field-error]").text()).toBe("Input should be a valid number");
  });

  it("puts scalar fields in two columns and lists, objects and JSON across both", () => {
    const wide = {
      properties: {
        n: { title: "N", type: "integer" },
        ok: { title: "Ok", type: "boolean" },
        mode: { enum: ["a", "b"], title: "Mode", type: "string" },
        code: { maxLength: 8, title: "Code", type: "string" },
        image: { title: "Image", type: "string" },
        names: { items: { type: "string" }, title: "Names", type: "array" },
        extra: { additionalProperties: { type: "string" }, title: "Extra", type: "object" },
      },
      type: "object",
    };
    const wrapper = mount(SchemaForm, { props: { schema: wide, value: {} } });
    const span = (key: string) => wrapper.get(`[data-field='${key}']`).classes().includes("col-span-2");
    expect(["n", "ok", "mode", "code"].map(span)).toEqual([false, false, false, false]);
    expect(["image", "names", "extra"].map(span)).toEqual([true, true, true]);
    expect(wrapper.get("[data-form]").classes()).toContain("grid-cols-2");
  });
});
