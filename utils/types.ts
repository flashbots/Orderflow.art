// Select menu item
export type SelectItem = {
  value: string;
  label: string;
};

export type getSankeyDataResponse = {
  data?: {
    entityFilter: string;
    links: {
      source: number[];
      target: number[];
      value: number[];
    };
    labels: string[];
    colors: string[];
    range: {
      startTime: string;
      endTime: string;
      startBlock: string;
      endBlock: string;
    } | null;
  };
  error?: string;
};

export type getDataRangeResponse = {
  data?: {
    range: {
      startTime: string;
      endTime: string;
      startBlock: string;
      endBlock: string;
    } | null;
  };
  error?: string;
};

export type EntityName = "frontend" | "metaAggregator" | "solver" | "mempool" | "ofa" | "builder";

export type getEntitiesResponse = {
  entities?: Record<EntityName, { value: string; label: string }[]>;
  error?: string;
};

export type getFilteredPairsResponse = {
  pairs?: {
    ethbtcPairs: {
      value: string;
      label: string;
    }[];
    stableswapPairs: {
      value: string;
      label: string;
    }[];
    longtailPairs: {
      value: string;
      label: string;
    }[];
  };
  error?: string;
};

export type getHashesResponse = {
  hashes?: {
    value: string;
    label: string;
  }[];
  error?: string;
};

// Sankey diagram choices
export enum Sankey {
  Liquidity = "lq",
  Orderflow = "of",
}

export type Avatar = {
  path: string;
  twitter: string;
  spaceBefore?: boolean;
};

export type Entities = Record<EntityName, string[]>;
export type SelectItemEntities = Record<EntityName, SelectItem[]>;

export interface GroupedOption {
  readonly label: string;
  readonly options: readonly SelectItem[];
}

export type SeparatedPairs = {
  ethbtcPairs: { selected: SelectItem[]; unSelected: SelectItem[]; allSelected: boolean };
  stableswapPairs: { selected: SelectItem[]; unSelected: SelectItem[]; allSelected: boolean };
  longtailPairs: { selected: SelectItem[]; unSelected: SelectItem[]; allSelected: boolean };
};

export type LabelType = "ethbtcPairs" | "stableswapPairs" | "longtailPairs";
