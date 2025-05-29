import numpy as np
import pandas as pd
from urllib.parse import quote


class BackgroundSparklineGenerator:
    """Generates clean SVG sparklines for metric card backgrounds."""

    def __init__(self, card_width=280, card_height=120):
        self.card_width = card_width
        self.card_height = card_height

    def generate_wealth_sparkline(self, time_series_data):
        """Generate background sparkline for total wealth card - bulletproof version."""

        try:
            # Step 1: Get clean, valid data
            clean_data = self._get_clean_wealth_data(time_series_data)

            if len(clean_data) < 2:
                return self._create_fallback_svg()

            # Step 2: Create sparkline coordinates (guaranteed no NaN)
            coordinates = self._create_sparkline_coordinates(
                clean_data, curve_type="wealth"
            )

            # Step 3: Generate SVG
            return self._create_svg_from_coordinates(coordinates, svg_type="wealth")

        except Exception as e:
            print(f"⚠️  Wealth sparkline generation failed: {e}")
            return self._create_fallback_svg()

    def generate_billionaire_count_sparkline(self, time_series_data):
        """Generate sparkline for billionaire count - shows monopolization by more people."""

        try:
            clean_data = self._get_clean_count_data(time_series_data)
            if len(clean_data) < 2:
                return self._create_fallback_svg()

            coordinates = self._create_count_coordinates(clean_data)
            return self._create_svg_from_coordinates(coordinates, svg_type="count")

        except Exception as e:
            print(f"⚠️  Count sparkline generation failed: {e}")
            return self._create_fallback_svg()

    def generate_average_wealth_sparkline(self, time_series_data):
        """Generate sparkline for average billionaire wealth - individual fortune growth."""

        try:
            clean_data = self._get_clean_average_data(time_series_data)
            if len(clean_data) < 2:
                return self._create_fallback_svg()

            coordinates = self._create_sparkline_coordinates(
                clean_data, curve_type="moderate"
            )
            return self._create_svg_from_coordinates(coordinates, svg_type="average")

        except Exception as e:
            print(f"⚠️  Average sparkline generation failed: {e}")
            return self._create_fallback_svg()

    def _get_clean_wealth_data(self, time_series_data):
        """Extract and clean wealth data - full dataset with rolling average and sampling."""

        # Step 1: Use ALL available data to show complete trend
        full_data = time_series_data["total_wealth"].dropna()

        if len(full_data) < 10:
            # Fallback for insufficient data
            return pd.Series([10.0, 12.0, 14.0, 16.0, 16.6])

        # Step 2: Apply rolling average to smooth out noise
        # Use window size based on data length (more data = larger window)
        window_size = max(3, len(full_data) // 20)  # ~5% of data points
        smoothed_data = full_data.rolling(window=window_size, center=True).mean()

        # Fill NaN values at edges from rolling average
        smoothed_data = smoothed_data.bfill().ffill()

        # Step 3: Resample to exactly 70 points for optimal visual clarity
        target_points = 70

        if len(smoothed_data) <= target_points:
            # If we have fewer points than target, use all of them
            final_data = smoothed_data
        else:
            # Resample to exactly 70 points using interpolation
            final_data = self._resample_data(smoothed_data, target_points)

        print(f"🔧 Full dataset processing:")
        print(f"   📊 Original data: {len(full_data)} points")
        print(f"   📈 Rolling average window: {window_size} points")
        print(f"   🎯 Final sampled data: {len(final_data)} points")
        print(f"   📉 Value range: {final_data.min():.1f}T to {final_data.max():.1f}T")
        print(
            f"   🚀 Total growth: {((final_data.iloc[-1] / final_data.iloc[0] - 1) * 100):.1f}%"
        )

        return final_data

    def _get_clean_count_data(self, time_series_data):
        """Extract and clean billionaire count data."""

        full_data = time_series_data["billionaire_count"].dropna()

        if len(full_data) < 10:
            return pd.Series([2500, 2600, 2700, 2800, 2900, 3000, 3024])

        # Count data is typically less noisy, use smaller smoothing window
        window_size = max(2, len(full_data) // 25)  # ~4% of data points
        smoothed_data = full_data.rolling(window=window_size, center=True).mean()
        smoothed_data = smoothed_data.bfill().ffill()

        # Resample to 70 points
        final_data = self._resample_data(smoothed_data, 70)

        print(f"🔧 Billionaire count processing:")
        print(f"   📊 Original: {len(full_data)} points")
        print(f"   📈 Window: {window_size} points")
        print(f"   🎯 Final: {len(final_data)} points")
        print(
            f"   👥 Count range: {final_data.min():.0f} to {final_data.max():.0f} billionaires"
        )

        return final_data

    def _get_clean_average_data(self, time_series_data):
        """Extract and clean average billionaire wealth data."""

        # Calculate average wealth per billionaire
        wealth_data = time_series_data["total_wealth"].dropna()
        count_data = time_series_data["billionaire_count"].dropna()

        # Align the data by taking common indices
        common_indices = wealth_data.index.intersection(count_data.index)
        if len(common_indices) < 10:
            return pd.Series([4.0, 4.2, 4.5, 4.8, 5.1, 5.3, 5.5])

        aligned_wealth = wealth_data.loc[common_indices]
        aligned_count = count_data.loc[common_indices]

        # Calculate average (avoid division by zero)
        average_data = aligned_wealth / aligned_count.replace(0, 1)

        # Smooth and resample
        window_size = max(3, len(average_data) // 20)
        smoothed_data = average_data.rolling(window=window_size, center=True).mean()
        smoothed_data = smoothed_data.bfill().ffill()

        final_data = self._resample_data(smoothed_data, 70)

        print(f"🔧 Average wealth processing:")
        print(f"   📊 Original: {len(average_data)} points")
        print(f"   📈 Window: {window_size} points")
        print(f"   🎯 Final: {len(final_data)} points")
        print(
            f"   💰 Average range: ${final_data.min():.1f}B to ${final_data.max():.1f}B"
        )

        return final_data

    def _resample_data(self, data, target_points):
        """Resample data to target number of points using interpolation."""

        if len(data) <= target_points:
            return data

        original_indices = np.arange(len(data))
        target_indices = np.linspace(0, len(data) - 1, target_points)
        final_values = np.interp(target_indices, original_indices, data.values)

        return pd.Series(final_values)

    def _create_sparkline_coordinates(self, data, curve_type="wealth"):
        """Create sparkline coordinates with different visual emphasis per metric type."""

        # Normalize data to fit card dimensions with padding
        padding = 15  # Slightly less padding to show more of the curve
        data_min, data_max = data.min(), data.max()

        # Avoid division by zero
        if data_max == data_min:
            data_range = 1.0
            normalized_data = [0.5] * len(data)  # Flat line in middle
        else:
            data_range = data_max - data_min
            normalized_data = [(val - data_min) / data_range for val in data]

        # Apply different curve emphasis based on metric type
        if curve_type == "wealth":
            # Exponential emphasis for wealth (shows acceleration)
            curve_data = [val**0.85 for val in normalized_data]
        elif curve_type == "moderate":
            # Moderate emphasis for average wealth
            curve_data = [val**0.95 for val in normalized_data]
        elif curve_type == "dramatic":
            # Strong emphasis for growth rate (volatile data)
            curve_data = [val**0.75 for val in normalized_data]
        else:
            # Linear for count data (steady growth)
            curve_data = normalized_data

        # Convert to card coordinates
        coordinates = []
        for i, norm_val in enumerate(curve_data):
            # X coordinate (spread evenly across card width)
            x = (i / (len(curve_data) - 1)) * self.card_width

            # Y coordinate (flip and add padding)
            y_normalized = padding + norm_val * (self.card_height - 2 * padding)
            y = self.card_height - y_normalized  # Flip Y axis

            # Ensure coordinates are valid numbers
            x = float(x) if not np.isnan(x) else 0.0
            y = float(y) if not np.isnan(y) else self.card_height / 2

            coordinates.append((x, y))

        print(f"🔧 Generated {len(coordinates)} coordinates ({curve_type} curve)")
        print(
            f"🔧 Curve span: ({coordinates[0][0]:.1f}, {coordinates[0][1]:.1f}) → ({coordinates[-1][0]:.1f}, {coordinates[-1][1]:.1f})"
        )

        return coordinates

    def _create_count_coordinates(self, data):
        """Create coordinates specifically for count data (stepped appearance)."""

        # Count data should look more stepped/discrete
        padding = 15
        data_min, data_max = data.min(), data.max()

        if data_max == data_min:
            normalized_data = [0.5] * len(data)
        else:
            data_range = data_max - data_min
            normalized_data = [(val - data_min) / data_range for val in data]

        # Create stepped appearance by slightly flattening segments
        stepped_data = []
        for i, val in enumerate(normalized_data):
            if i > 0 and i < len(normalized_data) - 1:
                # Average with neighbors for smoother steps
                avg_val = (normalized_data[i - 1] + val + normalized_data[i + 1]) / 3
                stepped_data.append(avg_val)
            else:
                stepped_data.append(val)

        # Convert to coordinates
        coordinates = []
        for i, norm_val in enumerate(stepped_data):
            x = (i / (len(stepped_data) - 1)) * self.card_width
            y_normalized = padding + norm_val * (self.card_height - 2 * padding)
            y = self.card_height - y_normalized

            x = float(x) if not np.isnan(x) else 0.0
            y = float(y) if not np.isnan(y) else self.card_height / 2

            coordinates.append((x, y))

        print(f"🔧 Generated {len(coordinates)} stepped coordinates for count data")

        return coordinates

    def _create_svg_from_coordinates(self, coordinates, svg_type="wealth"):
        """Create SVG polygon from coordinates with type-specific styling."""

        # Define colors for each metric type
        color_schemes = {
            "wealth": {"light": "#404040", "dark": "#1a1a1a"},  # Strong contrast
            "count": {"light": "#3a3a3a", "dark": "#222222"},  # Moderate contrast
            "average": {"light": "#383838", "dark": "#1f1f1f"},  # Subtle contrast
            "growth": {"light": "#424242", "dark": "#181818"},  # High contrast
        }

        colors = color_schemes.get(svg_type, color_schemes["wealth"])

        # Create bottom boundary points for closed polygon
        bottom_coords = [
            (0, self.card_height),  # Bottom-left
            *coordinates,  # Sparkline curve
            (self.card_width, self.card_height),  # Bottom-right
        ]

        # Convert to polygon points string
        points_list = []
        for x, y in bottom_coords:
            # Double-check for valid numbers
            if np.isnan(x) or np.isnan(y):
                print(f"⚠️  Found NaN coordinate, skipping")
                continue
            points_list.append(f"{x:.1f},{y:.1f}")

        points_str = " ".join(points_list)

        print(f"🔧 Final {svg_type} polygon: {len(points_list)} points")

        # Clean SVG with type-specific colors
        svg_content = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.card_width} {self.card_height}">
            <!-- Light background -->
            <rect width="100%" height="100%" fill="{colors['light']}"/>
            
            <!-- Dark area below sparkline -->
            <polygon points="{points_str}" fill="{colors['dark']}" stroke="none"/>
        </svg>"""

        # URL encode
        encoded_svg = quote(svg_content.replace("\n", "").replace("  ", ""))
        return f"data:image/svg+xml,{encoded_svg}"

    def _create_fallback_svg(self):
        """Create fallback SVG showing realistic wealth growth pattern."""

        # Create a realistic exponential growth curve as fallback
        # Simulates the wealth monopolization story
        width_points = np.linspace(0, self.card_width, 15)
        height_points = []

        for i, x in enumerate(width_points):
            # Exponential growth pattern (starts slow, accelerates)
            progress = i / (len(width_points) - 1)
            # Exponential curve: starts at 80% height, ends at 20% height (wealth growing up)
            y = self.card_height * (0.8 - 0.6 * (progress**2))
            height_points.append(y)

        # Create polygon points
        points = []
        for x, y in zip(width_points, height_points):
            points.append(f"{x:.1f},{y:.1f}")

        # Add bottom corners
        points = (
            [f"0,{self.card_height}"]
            + points
            + [f"{self.card_width},{self.card_height}"]
        )
        points_str = " ".join(points)

        svg_content = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.card_width} {self.card_height}">
            <rect width="100%" height="100%" fill="#404040"/>
            <polygon points="{points_str}" fill="#1a1a1a" stroke="none"/>
        </svg>"""

        encoded_svg = quote(svg_content.replace("\n", "").replace("  ", ""))
        return f"data:image/svg+xml,{encoded_svg}"

    def generate_all_background_sparklines(self, dashboard_data):
        """Generate background sparklines for metric cards that make sense."""

        sparklines = {}

        if "time_series" in dashboard_data and len(dashboard_data["time_series"]) > 0:
            print("🎨 Generating sparklines for metric cards...")

            # Total wealth sparkline (primary metric)
            sparklines["total_wealth"] = self.generate_wealth_sparkline(
                dashboard_data["time_series"]
            )

            # Billionaire count sparkline
            sparklines["billionaire_count"] = self.generate_billionaire_count_sparkline(
                dashboard_data["time_series"]
            )

            # Average wealth sparkline
            sparklines["average_wealth"] = self.generate_average_wealth_sparkline(
                dashboard_data["time_series"]
            )

            # No sparkline for doubling time - it's a calculated metric, not time series data

            print("✅ All relevant sparklines generated successfully!")

        else:
            print("⚠️  No time series data available, using fallbacks")
            fallback_svg = self._create_fallback_svg()
            sparklines["total_wealth"] = fallback_svg
            sparklines["billionaire_count"] = fallback_svg
            sparklines["average_wealth"] = fallback_svg

        return sparklines
