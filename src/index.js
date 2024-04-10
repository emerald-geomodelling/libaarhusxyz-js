import _ from "underscore";

// Compare python methods with the same names in libaarhusxyz.xyz!
// Please keep these in sync!
export const diff_df = (a, b) => {
  const difflen = Math.min(
    Object.values(a)[0].length,
    Object.values(b)[0].length
  );
  const rows = new Int8Array(
    Math.max(Object.values(a)[0].length, Object.values(b)[0].length)
  );
  rows.fill(1, difflen);
  const cols = {};

  _.union(Object.keys(a), Object.keys(a)).forEach((col) => {
    if (a[col] === undefined || b[col] === undefined) {
      rows.fill(1);
      cols[col] = true;
    } else {
      var any = false;
      for (var idx = 0; idx < difflen; idx++) {
        if (
          !(
            a[col][idx] === b[col][idx] ||
            (typeof a[col][idx] === "number" &&
              typeof b[col][idx] === "number" &&
              isNaN(a[col][idx]) &&
              isNaN(b[col][idx]))
          )
        ) {
          any = true;
          rows[idx] = 1;
        }
      }
      if (any) {
        cols[col] = true;
      }
    }
  });

  return [rows, cols];
};

export const extract_df = (df, rows, cols, annotate) => {
  if (rows.length > Object.values(df)[0].length) {
    throw Error("Can not shorten dataframes");
  } else {
    df = Object.fromEntries(
      Object.entries(df)
        .filter(([key, col]) => !!col.filter)
        .map(([key, col]) => [key, col.filter((item, idx) => rows[idx])])
    );
  }
  df = Object.fromEntries(
    Object.entries(df).filter(([key, value]) => cols[key] !== undefined)
  );
  Object.keys(cols).forEach((col) => {
    if (df[col] === undefined) {
      df[col] = new Int8Array(rows.length);
      df[col].fill(NaN);
    }
  });
  if (annotate) {
    df["apply_idx"] = new Int32Array(rows)
      .map((use, idx) => (use ? idx : -1))
      .filter((idx) => idx >= 0);
  }
  return df;
};

export const diff = (self, other) => {
  const [rows, flightlines_cols] = diff_df(self.flightlines, other.flightlines);

  const datasets = _.union(
    Object.keys(self.layer_data),
    Object.keys(other.layer_data)
  );

  const layer_data = {};

  datasets.forEach((dataset) => {
    if (self.layer_data[dataset] === undefined) {
      rows.fill(1);
      layer_data[dataset] = Object.keys(other.layer_data[dataset]);
    } else if (other.layer_data[dataset] === undefined) {
      rows.fill(1);
      layer_data[dataset] = Object.keys(self.layer_data[dataset]);
    } else {
      const [r, c] = diff_df(
        self.layer_data[dataset],
        other.layer_data[dataset]
      );

      if (Object.keys(c).length > 0) {
        let any = false;
        for (var idx = 0; idx < r.length; idx++) {
          if (r[idx]) {
            rows[idx] = 1;
            any = true;
          }
        }
        if (any) {
          layer_data[dataset] = c;
        }
      }
    }
  });

  return {
    model_info: {
      diff_a_source: self.model_info.source || "",
      diff_b_source: other.model_info.source || "",
    },
    flightlines: extract_df(other.flightlines, rows, flightlines_cols, true),
    layer_data: Object.fromEntries(
      Object.entries(layer_data).map(([dataset, cols]) => {
        if (other.layer_data[dataset] !== undefined) {
          return [dataset, extract_df(other.layer_data[dataset], rows, cols)];
        } else {
          return [dataset, null];
        }
      })
    ),
  };
};

export const apply_diff = (self, diff) => {
  const res = structuredClone(self);

  const rows = diff.flightlines.apply_idx;
  const outlength = Object.values(res.flightlines)[0].length;
  
  const df_apply = (df, diffdf, rows) => {
    Object.keys(diffdf).forEach((col) => {
      if (col !== "apply_idx") {
        if (df[col] === undefined) {
          const ColType = diffdf[col].constructor;
          df[col] = new ColType(outlength);
          if (ColType.name.indexOf("Float") !== -1) {
            df[col] = df[col].map((a) => NaN);
          }
        }
        for (var idx = 0; idx < rows.length; idx++) {
          df[col][rows[idx]] = diffdf[col][idx];
        }
      }
    });
  };

  df_apply(res.flightlines, diff.flightlines, rows);

  Object.entries(diff.layer_data).forEach(([dataset, datasetdiff]) => {
    if (datasetdiff === null) {
      if (res.layer_data[dataset] !== undefined) {
        delete res.layer_data[dataset];
      }
    } else {
      if (res.layer_data[dataset] === undefined) {
        res.layer_data[dataset] = {};
      }
      df_apply(res.layer_data[dataset], datasetdiff, rows);
    }
  });

  return res;
};
