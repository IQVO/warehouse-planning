import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebar: SidebarsConfig = {
  apisidebar: [
    {
      type: "doc",
      id: "api-reference/rest/warehouse-planning",
    },
    {
      type: "category",
      label: "process-capacities",
      link: {
        type: "doc",
        id: "api-reference/rest/process-capacities",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/register-process-capacity-constraint",
          label: "Register (or upsert) one capacity constraint",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-effective-process-capacity",
          label: "Get the effective capacity for a process, location and window",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "process-paths",
      link: {
        type: "doc",
        id: "api-reference/rest/process-paths",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/list-process-paths",
          label: "List the registered ProcessPaths",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/register-process-path",
          label: "Register (seed) a ProcessPath read model",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-process-path-capacity",
          label: "Get a ProcessPath's normalized, end-to-end capacity",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "capacity-plans",
      link: {
        type: "doc",
        id: "api-reference/rest/capacity-plans",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/list-capacity-plans",
          label: "List the most recent CapacityPlans",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/create-capacity-plan",
          label: "Create a DRAFT CapacityPlan and compute its shortage",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-capacity-plan",
          label: "Get a CapacityPlan",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/publish-capacity-plan",
          label: "Publish a DRAFT CapacityPlan",
          className: "api-method post",
        },
      ],
    },
    {
      type: "category",
      label: "station-capacity",
      link: {
        type: "doc",
        id: "api-reference/rest/station-capacity",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/declare-station-standard",
          label: "Declare the throughput of ONE station of a process at a site",
          className: "api-method put",
        },
        {
          type: "doc",
          id: "api-reference/rest/list-station-standards",
          label: "List the declared station standards",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-storage-capacity",
          label: "Storage positions and stations of a site (read model)",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "demand",
      link: {
        type: "doc",
        id: "api-reference/rest/demand",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/get-expected-demand",
          label: "Expected demand of a site over a window (order-management read model)",
          className: "api-method get",
        },
      ],
    },
  ],
};

export default sidebar.apisidebar;
