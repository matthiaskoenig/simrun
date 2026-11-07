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
});
